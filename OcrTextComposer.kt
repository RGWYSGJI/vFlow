package com.chaomixian.vflow.ocr

/**
 * 把 OCR 的逐框结果重排成「视觉行 → 段落」。
 *
 * 背景：PP-OCRv5 返回的是文本行框，同一视觉行经常被拆成多个框（按钮、标签、表格单元格），
 * 直接 `joinToString("\n")` 会让一句话被切成多行；英文单词跨框时又缺少空格。
 * 这里按几何位置聚行、判断软换行，再拼成段落。
 *
 * 纯 Kotlin，不依赖 Android 框架，便于单元测试。
 */
data class OcrTextBox(
    val text: String,
    val left: Int,
    val top: Int,
    val right: Int,
    val bottom: Int,
) {
    val width: Int get() = right - left
    val height: Int get() = bottom - top
    val centerY: Int get() = (top + bottom) / 2
}

data class OcrTextLayout(
    /** 视觉行：同一水平带的框已合并，按阅读顺序排列 */
    val lines: List<String>,
    /** 段落：软换行的视觉行已接回去 */
    val paragraphs: List<String>,
) {
    val fullText: String get() = paragraphs.joinToString("\n")

    companion object {
        val EMPTY = OcrTextLayout(emptyList(), emptyList())
    }
}

object OcrTextComposer {

    fun compose(boxes: List<OcrTextBox>): OcrTextLayout {
        val usable = boxes.filter { it.text.isNotBlank() && it.width > 0 && it.height > 0 }
        if (usable.isEmpty()) return OcrTextLayout.EMPTY

        val lines = buildVisualLines(usable)
        val lineTexts = lines.map { joinBoxes(it.boxes) }
        return OcrTextLayout(
            lines = lineTexts,
            paragraphs = buildParagraphs(lines, lineTexts),
        )
    }

    // ---------------------------------------------------------------- 行内

    private fun buildVisualLines(boxes: List<OcrTextBox>): List<VisualLine> {
        val sorted = boxes.sortedWith(compareBy({ it.top }, { it.left }, { it.right }))
        val lines = ArrayList<VisualLine>()
        for (box in sorted) {
            val line = lines.firstOrNull { it.accepts(box) }
            if (line == null) {
                lines.add(VisualLine(box))
            } else {
                line.add(box)
            }
        }
        lines.sortWith(compareBy({ it.top }, { it.left }))
        for (line in lines) {
            line.boxes.sortWith(compareBy({ it.left }, { it.centerY }))
        }
        return lines
    }

    private fun joinBoxes(boxes: List<OcrTextBox>): String {
        val builder = StringBuilder()
        var previous: String? = null
        for (box in boxes) {
            val text = box.text.trim()
            if (text.isEmpty()) continue
            if (previous != null && needsSpace(previous, text)) builder.append(' ')
            builder.append(text)
            previous = text
        }
        return builder.toString()
    }

    // ---------------------------------------------------------------- 段落

    private fun buildParagraphs(lines: List<VisualLine>, texts: List<String>): List<String> {
        if (lines.isEmpty()) return emptyList()

        val paragraphs = ArrayList<String>()
        val current = StringBuilder(texts.first())
        for (index in 1 until lines.size) {
            val previousLine = lines[index - 1]
            val line = lines[index]
            val previousText = texts[index - 1]
            val text = texts[index]

            if (startsNewParagraph(previousLine, line, previousText, text)) {
                paragraphs.add(current.toString())
                current.setLength(0)
                current.append(text)
            } else {
                if (needsSpace(previousText, text)) current.append(' ')
                current.append(text)
            }
        }
        paragraphs.add(current.toString())
        return paragraphs.filter { it.isNotBlank() }
    }

    private fun startsNewParagraph(
        previousLine: VisualLine,
        line: VisualLine,
        previousText: String,
        text: String,
    ): Boolean {
        val averageHeight = (previousLine.height + line.height) / 2f
        if (averageHeight <= 0f) return true

        // 行距明显大于行高：新的列表项 / 卡片 / 段落。
        val verticalGap = (line.top - previousLine.bottom).toFloat()
        if (verticalGap > averageHeight * PARAGRAPH_MIN_VERTICAL_GAP) return true

        // 字号突变：标题与正文、正文与角标。
        if (previousLine.height > line.height * PARAGRAPH_MAX_HEIGHT_RATIO) return true
        if (line.height > previousLine.height * PARAGRAPH_MAX_HEIGHT_RATIO) return true

        // 列表标记开头：独立成段。
        if (startsWithListMarker(text)) return true

        // 上一行是短标题且明显更窄：独立成段。
        return previousText.length <= SHORT_LINE_MAX_CHARS &&
                previousLine.width < line.width * SHORT_LINE_MAX_WIDTH_RATIO
    }

    private fun startsWithListMarker(text: String): Boolean {
        val first = text.firstOrNull() ?: return false
        if (first in LIST_MARKERS) return true
        return first.isDigit() && text.length >= 2 && text[1] in NUMBERED_MARKERS
    }

    // ---------------------------------------------------------------- 文本拼接

    private fun needsSpace(previous: String, next: String): Boolean {
        if (previous.isEmpty() || next.isEmpty()) return false
        val last = previous.last()
        val first = next.first()
        if (last.isWhitespace() || first.isWhitespace()) return false
        if (first.isClosingPunctuation() || last.isOpeningPunctuation()) return false
        // 中文/日文/韩文之间不加空格。
        if (last.isCjk() || first.isCjk()) return false
        return last.isWordChar() && first.isWordChar()
    }

    private fun Char.isWordChar(): Boolean =
        isLetterOrDigit() || this == '$' || this == '%' || this == '#' || this == '@'

    private fun Char.isCjk(): Boolean =
        code in 0x3400..0x9FFF ||
                code in 0xF900..0xFAFF ||
                code in 0x3040..0x30FF ||
                code in 0xAC00..0xD7AF ||
                code in 0x3000..0x303F

    private fun Char.isClosingPunctuation(): Boolean = this in CLOSING_PUNCTUATION

    private fun Char.isOpeningPunctuation(): Boolean = this in OPENING_PUNCTUATION

    // ---------------------------------------------------------------- 结构

    private class VisualLine(first: OcrTextBox) {
        val boxes = mutableListOf(first)

        var left = first.left
            private set
        var right = first.right
            private set
        var top = first.top
            private set
        var bottom = first.bottom
            private set

        val width: Int get() = right - left
        val height: Int get() = bottom - top
        val centerY: Int get() = (top + bottom) / 2

        fun accepts(box: OcrTextBox): Boolean {
            if (height <= 0 || box.height <= 0) return false
            val overlap = minOf(bottom, box.bottom) - maxOf(top, box.top)
            val denominator = minOf(height, box.height).toFloat()
            val overlapRatio = overlap.coerceAtLeast(0) / denominator
            val centerDistance = kotlin.math.abs(box.centerY - centerY).toFloat()
            return overlapRatio >= SAME_LINE_MIN_VERTICAL_OVERLAP ||
                    centerDistance <= maxOf(height, box.height) * SAME_LINE_MAX_CENTER_DISTANCE
        }

        fun add(box: OcrTextBox) {
            boxes.add(box)
            left = minOf(left, box.left)
            right = maxOf(right, box.right)
            top = minOf(top, box.top)
            bottom = maxOf(bottom, box.bottom)
        }
    }
}

// ---------------------------------------------------------------- 常量

/** 垂直重叠比例达到该值，两个框视为同一行。 */
private const val SAME_LINE_MIN_VERTICAL_OVERLAP = 0.45f

/** 中心线距离不超过行高的该比例，也视为同一行（处理高低不齐的框）。 */
private const val SAME_LINE_MAX_CENTER_DISTANCE = 0.35f

/** 行间距超过行高的该比例，判定为新段落。 */
private const val PARAGRAPH_MIN_VERTICAL_GAP = 0.45f

/** 相邻两行高度差超过该比例，判定为不同层级（标题/正文）。 */
private const val PARAGRAPH_MAX_HEIGHT_RATIO = 1.35f

/** 短标题的最大字符数。 */
private const val SHORT_LINE_MAX_CHARS = 16

/** 短标题宽度小于当前行宽的该比例时，独立成段。 */
private const val SHORT_LINE_MAX_WIDTH_RATIO = 0.65f

private val LIST_MARKERS = setOf('-', '*', '•', '·', '●', '▪')
private val NUMBERED_MARKERS = setOf('.', '、', ')', '）')
private val CLOSING_PUNCTUATION = setOf(
    '.', ',', ';', ':', '!', '?', ')', ']', '}',
    '，', '。', '；', '：', '！', '？', '）', '】', '》', '、',
)
private val OPENING_PUNCTUATION = setOf('(', '[', '{', '（', '【', '《')
