package com.chaomixian.vflow.ocr

import org.junit.Assert.assertEquals
import org.junit.Test

class OcrTextComposerTest {

    private fun box(text: String, left: Int, top: Int, right: Int, bottom: Int) =
        OcrTextBox(text, left, top, right, bottom)

    @Test
    fun `boxes on the same line are merged in reading order`() {
        val layout = OcrTextComposer.compose(
            listOf(
                box("World", 70, 0, 120, 20),
                box("Hello", 0, 0, 60, 20),
            )
        )
        assertEquals(listOf("Hello World"), layout.lines)
        assertEquals(listOf("Hello World"), layout.paragraphs)
        assertEquals("Hello World", layout.fullText)
    }

    @Test
    fun `cjk boxes on the same line are merged without spaces`() {
        val layout = OcrTextComposer.compose(
            listOf(
                box("你好", 0, 0, 40, 20),
                box("世界", 40, 0, 80, 20),
            )
        )
        assertEquals("你好世界", layout.fullText)
    }

    @Test
    fun `rows with a large gap become separate paragraphs`() {
        val layout = OcrTextComposer.compose(
            listOf(
                box("标题", 0, 0, 200, 40),
                box("正文内容", 0, 120, 200, 160),
            )
        )
        assertEquals(listOf("标题", "正文内容"), layout.lines)
        assertEquals(listOf("标题", "正文内容"), layout.paragraphs)
    }

    @Test
    fun `soft wrapped cjk lines are joined into one paragraph`() {
        val layout = OcrTextComposer.compose(
            listOf(
                box("这是一段很长的文字", 0, 0, 500, 20),
                box("继续写在第二行", 0, 25, 500, 45),
            )
        )
        assertEquals(2, layout.lines.size)
        assertEquals(listOf("这是一段很长的文字继续写在第二行"), layout.paragraphs)
    }

    @Test
    fun `latin soft wrapped lines get a space`() {
        val layout = OcrTextComposer.compose(
            listOf(
                box("The quick brown", 0, 0, 400, 20),
                box("fox jumps", 0, 25, 400, 45),
            )
        )
        assertEquals(listOf("The quick brown fox jumps"), layout.paragraphs)
    }

    @Test
    fun `short heading starts a new paragraph`() {
        val layout = OcrTextComposer.compose(
            listOf(
                box("设置", 0, 0, 100, 30),
                box("这里是一段明显更长的说明文字", 0, 40, 500, 70),
            )
        )
        assertEquals(listOf("设置", "这里是一段明显更长的说明文字"), layout.paragraphs)
    }

    @Test
    fun `list marker starts a new paragraph`() {
        val layout = OcrTextComposer.compose(
            listOf(
                box("下面是选项", 0, 0, 400, 20),
                box("- 选项一", 0, 25, 400, 45),
            )
        )
        assertEquals(listOf("下面是选项", "- 选项一"), layout.paragraphs)
    }

    @Test
    fun `blank boxes are ignored`() {
        val layout = OcrTextComposer.compose(
            listOf(
                box("   ", 0, 0, 10, 10),
                box("ok", 0, 20, 20, 40),
            )
        )
        assertEquals(listOf("ok"), layout.lines)
    }
}
