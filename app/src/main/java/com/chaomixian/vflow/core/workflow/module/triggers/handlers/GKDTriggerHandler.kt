package com.chaomixian.vflow.core.workflow.module.triggers.handlers

import android.content.Context
import android.view.accessibility.AccessibilityEvent
import android.view.accessibility.AccessibilityNodeInfo
import com.chaomixian.vflow.core.logging.DebugLogger
import com.chaomixian.vflow.core.types.complex.VScreenElement
import com.chaomixian.vflow.core.utils.StorageManager
import com.chaomixian.vflow.core.workflow.model.TriggerSpec
import com.chaomixian.vflow.core.workflow.module.triggers.*
import com.chaomixian.vflow.core.workflow.module.triggers.RuleExecutionState
import com.chaomixian.vflow.services.ServiceStateBus
import com.google.gson.GsonBuilder
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import okhttp3.OkHttpClient
import okhttp3.Request
import com.google.gson.JsonArray as GsonJsonArray
import com.google.gson.JsonElement as GsonJsonElement
import com.google.gson.JsonObject as GsonJsonObject
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import li.songe.json5.Json5
import kotlinx.serialization.json.boolean
import kotlinx.serialization.json.booleanOrNull
import kotlinx.serialization.json.int
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.long
import li.songe.selector.FastQuery
import li.songe.selector.MatchOption
import li.songe.selector.QueryContext
import li.songe.selector.Selector
import li.songe.selector.Transform
import java.io.File
import java.util.concurrent.CopyOnWriteArrayList

/**
 * GKD订阅触发器处理器
 * 解析 gkd 格式的订阅规则，监听页面变化并触发工作流
 */
class GKDTriggerHandler : ListeningTriggerHandler() {

    companion object {
        private const val TAG = "GKDTriggerHandler"
        private const val RULES_DIR = "gkd_rules"

        /** 规则加载失败后，多久再重试（避免每个窗口事件都重新下载/解析）。 */
        private const val LOAD_RETRY_INTERVAL_MS = 5 * 60 * 1000L
    }

    // 存储所有工作流的状态
    private val triggerStates = CopyOnWriteArrayList<GKDTriggerState>()

    // 规则加载失败的工作流 -> 失败时间，避免高频重试
    private val failedTriggerStates = java.util.concurrent.ConcurrentHashMap<String, Long>()

    // Transform 实例
    private val transform = createTransform()

    // 监听Job
    private var listeningJob: Job? = null

    override fun startListening(context: Context) {
        DebugLogger.d(TAG, "开始监听 GKD 订阅规则")

        syncTriggerStates()

        listeningJob = triggerScope.launch {
            // 只保留最新一条事件（ServiceStateBus 内部 conflate），串行匹配，
            // 避免 content-changed 高频事件把匹配队列撑爆。
            ServiceStateBus.gkdEventSignal.collect {
                val events = ServiceStateBus.takeGkdWindowEvents()
                for (windowEvent in events) {
                    try {
                        DebugLogger.d(TAG, "收到窗口事件: ${windowEvent.packageName} / ${windowEvent.className}")
                        checkAndTriggerWorkflows(
                            context = context,
                            packageName = windowEvent.packageName,
                            className = windowEvent.className,
                            event = windowEvent.event,
                        )
                    } finally {
                        windowEvent.event?.recycle()
                    }
                }
            }
        }
    }

    override fun stopListening(context: Context) {
        DebugLogger.d(TAG, "停止监听 GKD 订阅规则")
        listeningJob?.cancel()
        listeningJob = null
        triggerStates.clear()
    }

    /**
     * 同步 triggerStates 与 listeningTriggers
     */
    private fun syncTriggerStates() {
        val currentTriggerIds = listeningTriggers.map { it.triggerId }.toSet()

        triggerStates.removeAll { state ->
            !currentTriggerIds.contains(state.trigger.triggerId)
        }
        failedTriggerStates.keys.retainAll(currentTriggerIds)

        val now = System.currentTimeMillis()
        for (trigger in listeningTriggers) {
            if (triggerStates.none { it.trigger.triggerId == trigger.triggerId }) {
                val lastFailure = failedTriggerStates[trigger.triggerId]
                if (lastFailure != null && now - lastFailure < LOAD_RETRY_INTERVAL_MS) {
                    continue
                }
                val state = createTriggerState(trigger)
                if (state != null) {
                    failedTriggerStates.remove(trigger.triggerId)
                    triggerStates.add(state)
                    DebugLogger.d(TAG, "加载工作流 '${trigger.workflowName}' 的 GKD 规则")
                } else {
                    failedTriggerStates[trigger.triggerId] = now
                    DebugLogger.w(TAG, "工作流 '${trigger.workflowName}' 规则加载失败，${LOAD_RETRY_INTERVAL_MS / 1000}s 后重试")
                }
            }
        }
    }

    /**
     * 创建触发器状态
     */
    private fun createTriggerState(trigger: TriggerSpec): GKDTriggerState? {
        val config = trigger.parameters

        val rules = mutableListOf<ResolvedGKDRule>()

        // 1. 从订阅 URL 下载
        val subscriptionUrl = config["subscriptionUrl"] as? String
        if (!subscriptionUrl.isNullOrBlank()) {
            DebugLogger.d(TAG, "工作流 '${trigger.workflowName}' 从订阅 URL 加载: $subscriptionUrl")
            val parsedRules = downloadAndParseSubscription(subscriptionUrl, getRulesDir())
            DebugLogger.d(TAG, "工作流 '${trigger.workflowName}' 解析到 ${parsedRules.size} 条规则")
            rules.addAll(parsedRules)
        }

        // 2. 从文件加载
        val subscriptionFile = config["subscriptionFile"] as? String
        if (!subscriptionFile.isNullOrBlank()) {
            val fileRules = loadRulesFromFile(subscriptionFile)
            rules.addAll(fileRules)
        }

        if (rules.isEmpty()) {
            DebugLogger.w(TAG, "工作流 '${trigger.workflowName}' 没有有效的 GKD 规则")
            return null
        }

        // 触发器面板上的 5 个时间参数是“规则未声明时的默认值”，
        // 规则自己的字段优先（对齐 GKD 的 rule -> group -> 默认值 合并顺序）。
        fun longParam(key: String): Long? = (config[key] as? Number)?.toLong()
        fun intParam(key: String): Int? = (config[key] as? Number)?.toInt()

        val defaultActionCd = longParam("actionCd")
        val defaultActionDelay = longParam("actionDelay")
        val defaultMatchTime = longParam("matchTime")
        val defaultMatchDelay = longParam("matchDelay")
        val defaultActionMaximum = intParam("actionMaximum")

        val rulesWithDefaults = rules.map { rule ->
            rule.copy(
                actionCd = rule.actionCd ?: defaultActionCd,
                actionDelay = rule.actionDelay ?: defaultActionDelay,
                matchTime = rule.matchTime ?: defaultMatchTime,
                matchDelay = rule.matchDelay ?: defaultMatchDelay,
                actionMaximum = rule.actionMaximum ?: defaultActionMaximum,
            )
        }

        return GKDTriggerState(trigger = trigger, rules = rulesWithDefaults)
    }

    /**
     * 下载并解析远程订阅
     * 使用缓存避免重复下载
     */
    private fun downloadAndParseSubscription(url: String, rulesDir: File): List<ResolvedGKDRule> {
        // 生成缓存文件名（使用 URL 的 hash 作为文件名）
        val fileName = "subscription_${url.hashCode()}.json"
        val file = File(rulesDir, fileName)

        // 检查缓存是否存在且有效（1小时内有效）
        val cacheValid = file.exists() && (System.currentTimeMillis() - file.lastModified()) < 3600000

        if (cacheValid) {
            DebugLogger.d(TAG, "使用缓存的订阅: ${file.absolutePath}")
            try {
                val content = file.readText()
                val parsed = if (content.isNotEmpty()) parseSubscriptionContent(content) else emptyList()
                if (parsed.isNotEmpty()) return parsed
                DebugLogger.w(TAG, "缓存内容无效（无规则），尝试重新下载")
            } catch (e: Exception) {
                DebugLogger.e(TAG, "读取缓存失败，尝试重新下载", e)
            }
        }

        // 缓存不存在或已过期，下载订阅
        return downloadSubscription(url, file)
    }

    /**
     * 实际执行下载操作
     * 在后台线程执行网络请求
     */
    private fun downloadSubscription(url: String, file: File): List<ResolvedGKDRule> {
        DebugLogger.d(TAG, "开始下载订阅: $url")

        return runBlocking(Dispatchers.IO) {
            try {
                val client = OkHttpClient.Builder().build()
                val request = Request.Builder().url(url).build()
                val response = client.newCall(request).execute()

                DebugLogger.d(TAG, "响应码: ${response.code}")

                if (!response.isSuccessful) {
                    DebugLogger.e(TAG, "下载订阅失败: ${response.code}")
                    return@runBlocking emptyList<ResolvedGKDRule>()
                }

                val body = response.body
                if (body == null) {
                    DebugLogger.e(TAG, "响应体为空")
                    return@runBlocking emptyList<ResolvedGKDRule>()
                }

                val content = body.string()
                DebugLogger.d(TAG, "下载内容长度: ${content.length}")

                if (content.isEmpty()) {
                    DebugLogger.e(TAG, "订阅内容为空")
                    return@runBlocking emptyList<ResolvedGKDRule>()
                }

                // 先解析；只有解析出规则才覆盖缓存，避免把 HTML/错误页写进去
                val parsed = parseSubscriptionContent(content)
                if (parsed.isEmpty()) {
                    DebugLogger.e(TAG, "下载内容无法解析出规则，保留旧缓存: $url")
                    if (file.exists()) {
                        val old = file.readText()
                        if (old.isNotBlank()) {
                            return@runBlocking parseSubscriptionContent(old)
                        }
                    }
                    return@runBlocking emptyList<ResolvedGKDRule>()
                }
                file.writeText(content)
                DebugLogger.d(TAG, "已保存订阅到: ${file.absolutePath}")
                parsed
            } catch (e: Exception) {
                DebugLogger.e(TAG, "下载订阅失败: ${e.message}", e)
                // 如果下载失败且有缓存文件，尝试使用缓存
                if (file.exists()) {
                    DebugLogger.d(TAG, "下载失败，尝试使用旧缓存")
                    return@runBlocking try {
                        val content = file.readText()
                        if (content.isNotEmpty()) {
                            parseSubscriptionContent(content)
                        } else {
                            emptyList()
                        }
                    } catch (e2: Exception) {
                        DebugLogger.e(TAG, "读取旧缓存也失败", e2)
                        emptyList()
                    }
                }
                emptyList()
            }
        }
    }

    /**
     * 解析订阅内容
     */
    private fun parseSubscriptionContent(content: String): List<ResolvedGKDRule> {
        // 验证输入
        val trimmed = content.trim()
        if (trimmed.isEmpty() || trimmed.length < 10) {
            DebugLogger.w(TAG, "订阅内容为空或过短")
            return emptyList()
        }

        // GitHub 的 issues / 网页链接会返回 HTML（HTTP 200），JSON5 解析必失败；
        // 提前识别并给出明确提示，避免把 HTML 写进缓存。
        if (trimmed.startsWith("<")) {
            DebugLogger.e(TAG, "订阅内容看起来是 HTML 页面（链接可能填错）: ${trimmed.take(80)}")
            return emptyList()
        }

        return try {
            // 使用 Json5 解析（支持单引号、注释等）
            val jsonElement = Json5.parseToJson5Element(trimmed)
            parseJsonElement(jsonElement)
        } catch (e: Exception) {
            DebugLogger.e(TAG, "解析订阅内容失败: ${trimmed.take(100)}", e)
            emptyList()
        }
    }

    /**
     * 解析 JSON 元素
     */
    private fun parseJsonElement(jsonElement: JsonElement): List<ResolvedGKDRule> {
        val rules = mutableListOf<ResolvedGKDRule>()

        if (jsonElement !is JsonObject) {
            DebugLogger.w(TAG, "JSON 根元素不是对象")
            return rules
        }
        val rootObj = jsonElement

        DebugLogger.d(TAG, "解析根对象，字段: ${rootObj.keys}")

        // 解析 apps
        val appsElement = rootObj["apps"]
        if (appsElement is JsonArray) {
            DebugLogger.d(TAG, "apps 数组元素数量: ${appsElement.size}")
            appsElement.forEachIndexed { index, appJson ->
                if (appJson !is JsonObject) {
                    DebugLogger.w(TAG, "app[$index] 不是对象")
                    return@forEachIndexed
                }
                val appObj = appJson

                // 检查 app 级别的启用状态
                val appEnable = appObj["enable"]?.jsonPrimitive?.booleanOrNull
                if (appEnable != null && !appEnable) {
                    DebugLogger.d(TAG, "app[$index] 已禁用，跳过")
                    return@forEachIndexed
                }

                val appId = appObj["id"]?.jsonPrimitive?.content ?: run {
                    DebugLogger.w(TAG, "app[$index] 缺少 id 字段")
                    return@forEachIndexed
                }
                val appName = appObj["name"]?.jsonPrimitive?.content

                val groupsElement = appObj["groups"]
                if (groupsElement is JsonArray) {
                    groupsElement.forEachIndexed { gIndex, groupJson ->
                        if (groupJson !is JsonObject) {
                            DebugLogger.w(TAG, "app[$appId].groups[$gIndex] 不是对象")
                            return@forEachIndexed
                        }
                        val groupObj = groupJson

                        // 检查 group 级别的启用状态
                        val groupEnable = groupObj["enable"]?.jsonPrimitive?.booleanOrNull
                        if (groupEnable != null && !groupEnable) {
                            DebugLogger.d(TAG, "app[$appId].groups[$gIndex] 已禁用，跳过")
                            return@forEachIndexed
                        }

                        val groupName = groupObj["name"]?.jsonPrimitive?.content ?: "未命名"

                        val rulesElement = groupObj["rules"]
                        if (rulesElement is JsonArray) {
                            rulesElement.forEachIndexed { rIndex, ruleJson ->
                                if (ruleJson !is JsonObject) {
                                    DebugLogger.w(TAG, "app[$appId].$groupName.rules[$rIndex] 不是对象")
                                    return@forEachIndexed
                                }
                                val ruleObj = ruleJson

                                // 检查 rule 级别的启用状态
                                val ruleEnable = ruleObj["enable"]?.jsonPrimitive?.booleanOrNull
                                if (ruleEnable != null && !ruleEnable) {
                                    DebugLogger.d(TAG, "app[$appId].$groupName.rules[$rIndex] 已禁用，跳过")
                                    return@forEachIndexed
                                }

                                val rule = parseRule(ruleObj, groupObj, groupName, appId)
                                if (rule != null) {
                                    rules.add(rule)
                                }
                            }
                        }
                    }
                }
            }
        } else {
            DebugLogger.w(TAG, "apps 字段不是数组或为空: ${appsElement?.javaClass?.simpleName}")
        }

        // 解析 globalGroups
        val globalGroupsElement = rootObj["globalGroups"]
        if (globalGroupsElement is JsonArray) {
            DebugLogger.d(TAG, "globalGroups 数组元素数量: ${globalGroupsElement.size}")
            globalGroupsElement.forEachIndexed { index, groupJson ->
                if (groupJson !is JsonObject) {
                    DebugLogger.w(TAG, "globalGroups[$index] 不是对象")
                    return@forEachIndexed
                }
                val groupObj = groupJson

                // 检查 globalGroup 级别的启用状态
                val groupEnable = groupObj["enable"]?.jsonPrimitive?.booleanOrNull
                if (groupEnable != null && !groupEnable) {
                    DebugLogger.d(TAG, "globalGroups[$index] 已禁用，跳过")
                    return@forEachIndexed
                }

                val groupName = groupObj["name"]?.jsonPrimitive?.content ?: "全局"

                val rulesElement = groupObj["rules"]
                if (rulesElement is JsonArray) {
                    rulesElement.forEachIndexed { rIndex, ruleJson ->
                        if (ruleJson !is JsonObject) {
                            DebugLogger.w(TAG, "globalGroups.$groupName.rules[$rIndex] 不是对象")
                            return@forEachIndexed
                        }
                        val ruleObj = ruleJson

                        // 检查 rule 级别的启用状态
                        val ruleEnable = ruleObj["enable"]?.jsonPrimitive?.booleanOrNull
                        if (ruleEnable != null && !ruleEnable) {
                            DebugLogger.d(TAG, "globalGroups.$groupName.rules[$rIndex] 已禁用，跳过")
                            return@forEachIndexed
                        }

                        val rule = parseRule(ruleObj, groupObj, groupName, null)
                        if (rule != null) {
                            rules.add(rule)
                        }
                    }
                }
            }
        }

        DebugLogger.d(TAG, "共解析到 ${rules.size} 条规则")
        return rules
    }

    /**
     * 解析规则
     */
    private fun parseRule(
        ruleObj: JsonObject,
        groupObj: JsonObject?,
        groupName: String,
        appId: String?
    ): ResolvedGKDRule? {
        // 规则字段优先，缺省回退到 group 级默认值（对齐 GKD 的 RuleExecutionParameters.resolve）
        fun field(name: String): JsonElement? = ruleObj[name] ?: groupObj?.get(name)

        val ruleName = ruleObj["name"]?.jsonPrimitive?.content ?: "未命名规则"
        val ruleKey = ruleObj["key"]?.jsonPrimitive?.int

        // 解析 preKeys（可能是单个数字或数组）
        val preKeys = when (val preKeysElement = ruleObj["preKeys"]) {
            null -> emptyList()
            is JsonArray -> preKeysElement.mapNotNull { it.jsonPrimitive.int }
            else -> {
                // 尝试作为单个数字解析
                listOfNotNull(preKeysElement.jsonPrimitive.int)
            }
        }

        // 解析 resetMatch
        val resetMatch = field("resetMatch")?.jsonPrimitive?.content

        // 解析选择器
        val matches = parseSelectors(ruleObj["matches"])
        val anyMatches = parseSelectors(ruleObj["anyMatches"])
        val excludeMatches = parseSelectors(ruleObj["excludeMatches"])
        val excludeAllMatches = parseSelectors(ruleObj["excludeAllMatches"])

        // 如果没有有效的选择器，跳过
        if (matches.isEmpty() && anyMatches.isEmpty()) {
            return null
        }

        return ResolvedGKDRule(
            name = ruleName,
            groupName = groupName,
            appId = appId,
            activityIds = parseStringArray(ruleObj["activityIds"]),
            excludeActivityIds = parseStringArray(ruleObj["excludeActivityIds"]),
            matches = matches,
            anyMatches = anyMatches,
            excludeMatches = excludeMatches,
            excludeAllMatches = excludeAllMatches,
            key = ruleKey,
            preKeys = preKeys,
            resetMatch = resetMatch,
            actionCd = field("actionCd")?.jsonPrimitive?.long,
            actionDelay = field("actionDelay")?.jsonPrimitive?.long,
            matchDelay = field("matchDelay")?.jsonPrimitive?.long,
            matchTime = field("matchTime")?.jsonPrimitive?.long,
            matchRoot = field("matchRoot")?.jsonPrimitive?.boolean,
            fastQuery = field("fastQuery")?.jsonPrimitive?.boolean,
            actionMaximum = field("actionMaximum")?.jsonPrimitive?.int,
            // 新增字段
            actionCdKey = field("actionCdKey")?.jsonPrimitive?.int,
            actionMaximumKey = field("actionMaximumKey")?.jsonPrimitive?.int,
            forcedTime = field("forcedTime")?.jsonPrimitive?.long,
            priorityTime = field("priorityTime")?.jsonPrimitive?.long,
            priorityActionMaximum = field("priorityActionMaximum")?.jsonPrimitive?.int,
            order = field("order")?.jsonPrimitive?.int ?: 0
        )
    }

    /**
     * 解析选择器列表
     */
    private fun parseSelectors(element: JsonElement?): List<Selector> {
        if (element == null) return emptyList()
        return when (element) {
            is JsonArray -> {
                element.mapNotNull { item ->
                    (item as? kotlinx.serialization.json.JsonPrimitive)?.content?.let { source ->
                        try {
                            Selector.parse(source)
                        } catch (e: Exception) {
                            DebugLogger.w(TAG, "解析选择器失败: $source")
                            null
                        }
                    }
                }
            }
            is kotlinx.serialization.json.JsonPrimitive -> {
                element.content?.let { source ->
                    try {
                        listOf(Selector.parse(source))
                    } catch (e: Exception) {
                        DebugLogger.w(TAG, "解析选择器失败: $source")
                        emptyList()
                    }
                } ?: emptyList()
            }
            else -> emptyList()
        }
    }

    /**
     * 解析字符串数组
     */
    private fun parseStringArray(element: JsonElement?): List<String>? {
        if (element == null) return null
        return when (element) {
            is JsonArray -> {
                element.mapNotNull { item -> (item as? kotlinx.serialization.json.JsonPrimitive)?.content }
            }
            is kotlinx.serialization.json.JsonPrimitive -> {
                element.content?.let { listOf(it) }
            }
            else -> null
        }
    }

    /**
     * 从文件加载规则
     */
    private fun loadRulesFromFile(filePath: String): List<ResolvedGKDRule> {
        val file = if (filePath.startsWith("/")) {
            File(filePath)
        } else {
            File(getRulesDir(), filePath)
        }

        if (!file.exists()) {
            DebugLogger.w(TAG, "规则文件不存在: ${file.absolutePath}")
            return emptyList()
        }

        return try {
            file.readText().let { content ->
                parseSubscriptionContent(content)
            }
        } catch (e: Exception) {
            DebugLogger.e(TAG, "读取规则文件失败: ${file.absolutePath}", e)
            emptyList()
        }
    }

    /**
     * 获取规则目录
     */
    private fun getRulesDir(): File {
        val dir = File(StorageManager.tempDir, RULES_DIR)
        if (!dir.exists()) dir.mkdirs()
        return dir
    }

    /**
     * 检查并触发匹配的工作流
     */
    private fun checkAndTriggerWorkflows(
        context: Context,
        packageName: String,
        className: String,
        event: AccessibilityEvent? = null,
    ) {
        syncTriggerStates()

        val service = ServiceStateBus.getAccessibilityService() ?: return
        val rootNode = service.rootInActiveWindow ?: return
        // GKD 的 matchRoot=false 是“从事件源节点开始匹配”；拿不到 source 时退回 root。
        val sourceNode = try {
            event?.source
        } catch (_: Exception) {
            null
        }

        val delayedScheduled = checkAndTriggerWithRoot(context, packageName, className, rootNode, sourceNode)
        if (!delayedScheduled) {
            // 没有延迟任务引用节点时才回收，避免 actionDelay 用到已回收的节点
            try {
                rootNode.recycle()
            } catch (_: Exception) {
            }
            if (sourceNode != null && sourceNode !== rootNode) {
                try {
                    sourceNode.recycle()
                } catch (_: Exception) {
                }
            }
        }
    }

    /**
     * @return 是否安排了 actionDelay 延迟任务（延迟任务会继续持有节点，此时不能回收 root/source）。
     */
    private fun checkAndTriggerWithRoot(
        context: Context,
        packageName: String,
        className: String,
        rootNode: AccessibilityNodeInfo,
        sourceNode: AccessibilityNodeInfo?,
    ): Boolean {
        var delayedScheduled = false
        val t = System.currentTimeMillis()
        val currentPackage = packageName
        // content-changed 的 className 经常是 View 类，不是 Activity；
        // 优先用该包最近一次窗口状态事件里的类名。
        val currentActivity = ServiceStateBus.activityClassFor(packageName) ?: className

        for (state in triggerStates) {
            try {
                // 检测应用/Activity 切换，处理 resetMatch
                val oldPackage = state.currentPackage
                val oldActivity = state.currentActivity

                if (oldPackage != currentPackage || oldActivity != currentActivity) {
                    // 应用或 Activity 发生变化
                    DebugLogger.d(TAG, "应用/Activity 切换: $oldPackage/$oldActivity -> $currentPackage/$currentActivity")

                    // 检查每个规则是否需要重置
                    for (rule in state.rules) {
                        if (rule.shouldReset(oldPackage, oldActivity, currentPackage, currentActivity)) {
                            DebugLogger.d(TAG, "规则 '${rule.name}' 需要重置 (resetMatch=${rule.resetMatch})")
                            state.resetRule(rule)

                            // 如果规则有 key，从 matchedKeys 中移除
                            if (rule.key != null) {
                                state.matchedKeys.remove(rule.key)
                                DebugLogger.d(TAG, "  移除已匹配的 key: ${rule.key}")
                            }
                        }
                    }

                    // 更新当前的应用和 Activity
                    state.currentPackage = currentPackage
                    state.currentActivity = currentActivity
                }

                // 匹配规则：priority 规则优先，其余按 order 排序（对齐 GKD）
                // 先算好 priority 再排序，避免比较器里读时钟导致排序契约不一致
                val orderedRules = state.rules
                    .map { rule -> rule to if (state.getRuleState(rule).isPriority()) 0 else 1 }
                    .sortedWith(compareBy<Pair<ResolvedGKDRule, Int>>({ it.second }, { it.first.order }))
                    .map { it.first }
                for (rule in orderedRules) {
                    val ruleState = state.getRuleState(rule)

                    // 检查规则状态（actionCdKey / actionMaximumKey 共享）
                    val sharedCooldown = rule.actionCdKey?.let { key ->
                        state.rules.firstOrNull { it.key == key }?.let { state.getRuleState(it).actionTriggerTime }
                    }
                    val sharedCount = rule.actionMaximumKey?.let { key ->
                        state.rules.firstOrNull { it.key == key }?.let { state.getRuleState(it).actionCount }
                    }
                    if (!ruleState.shouldTrigger(sharedCooldown, sharedCount)) {
                        if (rule.appId == null || rule.appId == currentPackage) {
                            DebugLogger.d(
                                TAG,
                                "规则 '${rule.name}' 状态=${ruleState.getStatus(sharedCooldown, sharedCount)} " +
                                    "(matchTime=${rule.matchTime}, changed=${ruleState.matchChangedTime})"
                            )
                        }
                        continue
                    }

                    // 检查前置规则 (preRules) - 用已匹配 key 集合，和 GKD 的 preRules 语义一致
                    if (!rule.checkPreKeys(state.matchedKeys)) {
                        DebugLogger.d(TAG, "规则 '${rule.name}' 的前置规则未触发 (preKeys=${rule.preKeys}, matchedKeys=${state.matchedKeys})")
                        continue
                    }

                    // 检查应用匹配
                    if (rule.appId != null && rule.appId != currentPackage) {
                        continue
                    }

                    // 检查 Activity 匹配
                    if (!matchActivity(rule.activityIds, rule.excludeActivityIds, currentActivity)) {
                        DebugLogger.d(TAG, "规则 '${rule.name}' Activity 不匹配: current=$currentActivity ids=${rule.activityIds}")
                        continue
                    }

                    DebugLogger.d(
                        TAG,
                        "检查规则 '${rule.name}': app=${rule.appId} activity=$currentActivity " +
                            "matches=${rule.matches.size} any=${rule.anyMatches.size} root=${rule.matchRoot}"
                    )

                    // 获取匹配选项
                    val matchOption = MatchOption(fastQuery = rule.fastQuery ?: false)
                    // 优先从 root 匹配（保持原来 VFlow 的行为），root 没命中再回退事件源节点。
                    // GKD 的语义是 matchRoot=false 从 source 匹配，但 VFlow 的规则/节点树
                    // 不一定满足这个前提，root 优先更稳。
                    val fallbackNode = if (rule.matchRoot == true) null else sourceNode
                    val targetNode = rootNode

                    // anyMatches 与 matches 是“与”关系：anyMatches 至少命中一个，matches 必须全部命中
                    // matchRoot=false 时先从事件源节点查；查不到再回退 root（兼容按整棵树写的规则）
                    var matchedNode: AccessibilityNodeInfo? = null
                    if (rule.anyMatches.isNotEmpty()) {
                        matchedNode = queryRuleSelectorsWithFallback(
                            targetNode, fallbackNode, rule.anyMatches, matchOption, false
                        ) ?: continue
                    }
                    if (rule.matches.isNotEmpty()) {
                        matchedNode = queryRuleSelectorsWithFallback(
                            targetNode, fallbackNode, rule.matches, matchOption, true
                        ) ?: continue
                    }
                    if (matchedNode == null) {
                        DebugLogger.d(TAG, "规则 '${rule.name}' 未命中（root/source 都没查到）")
                        continue
                    }

                    // 检查排除条件
                    if (queryRuleSelectors(targetNode, rule.excludeMatches, matchOption, false) != null) {
                        continue
                    }
                    if (rule.excludeAllMatches.isNotEmpty()) {
                        val allExcluded = rule.excludeAllMatches.all { selector ->
                            queryRuleSelectors(targetNode, listOf(selector), matchOption, false) == null
                        }
                        if (!allExcluded) {
                            continue
                        }
                    }

                    // 记录匹配
                    ruleState.recordMatch(t)

                    // 如果规则有 key，添加到 matchedKeys
                    if (rule.key != null) {
                        state.matchedKeys.add(rule.key)
                        DebugLogger.d(TAG, "规则 '${rule.name}' 匹配成功，添加 key: ${rule.key}")
                    }

                    // 处理触发延迟
                    if (ruleState.actionDelay > 0) {
                        ruleState.startActionDelay(t)
                        delayedScheduled = true
                        val job = triggerScope.launch {
                            delay(ruleState.actionDelay)
                            performTrigger(context, state, rule, ruleState, matchedNode)
                        }
                        ruleState.actionDelayJob = job
                    } else {
                        performTrigger(context, state, rule, ruleState, matchedNode)
                    }
                    break // 触发后退出规则循环
                }
            } catch (e: Exception) {
                DebugLogger.e(TAG, "处理工作流 '${state.trigger.workflowName}' 时出错", e)
            }
        }
        return delayedScheduled
    }

    private fun matchesActivityPattern(pattern: String, activity: String): Boolean {
        if (pattern == "*") return true
        return activity.startsWith(pattern)
    }

    /**
     * 匹配 Activity
     */
    private fun matchActivity(
        activityIds: List<String>?,
        excludeActivityIds: List<String>?,
        currentActivity: String
    ): Boolean {
        // 检查排除
        if (excludeActivityIds != null) {
            if (excludeActivityIds.any { pattern -> matchesActivityPattern(pattern, currentActivity) }) {
                return false
            }
        }

        // 如果没有指定 activityIds，则匹配所有
        if (activityIds == null) return true

        // 检查是否匹配（GKD 用前缀匹配，不是子串包含）
        return activityIds.any { pattern -> matchesActivityPattern(pattern, currentActivity) }
    }

    /**
     * 查询规则选择器匹配的节点
     * @param rootNode 根节点
     * @param selectors 选择器列表
     * @param matchOption 匹配选项
     * @param requireAll 是否要求全部匹配
     * @return 匹配的节点或 null
     */
    private fun queryRuleSelectors(
        targetNode: AccessibilityNodeInfo,
        selectors: List<Selector>,
        matchOption: MatchOption,
        requireAll: Boolean
    ): AccessibilityNodeInfo? {
        if (selectors.isEmpty()) return null

        return if (requireAll) {
            // 全部必须匹配
            var resultNode: AccessibilityNodeInfo? = null
            for (selector in selectors) {
                resultNode = querySingleSelector(targetNode, selector, matchOption) ?: return null
            }
            resultNode
        } else {
            // 任一匹配即可
            for (selector in selectors) {
                querySingleSelector(targetNode, selector, matchOption)?.let { return it }
            }
            null
        }
    }

    private fun queryRuleSelectorsWithFallback(
        primary: AccessibilityNodeInfo,
        fallback: AccessibilityNodeInfo?,
        selectors: List<Selector>,
        matchOption: MatchOption,
        requireAll: Boolean,
    ): AccessibilityNodeInfo? {
        val direct = queryRuleSelectors(primary, selectors, matchOption, requireAll)
        if (direct != null || fallback == null || fallback === primary) return direct
        return queryRuleSelectors(fallback, selectors, matchOption, requireAll)
    }

    /**
     * 单个选择器匹配：先匹配目标节点自身，再查后代；selector.isMatchRoot 时只匹配自身。
     * 同时回收“查询出来但用不到”的候选节点，避免节点池耗尽。
     */
    private fun querySingleSelector(
        targetNode: AccessibilityNodeInfo,
        selector: Selector,
        matchOption: MatchOption,
    ): AccessibilityNodeInfo? {
        val selfMatch = selector.match(targetNode, transform, matchOption)
        if (selfMatch != null) return selfMatch
        if (selector.isMatchRoot) return null

        val iterator = transform.querySelectorAll(targetNode, selector, matchOption).iterator()
        if (!iterator.hasNext()) return null
        val first = iterator.next()
        while (iterator.hasNext()) {
            val extra = iterator.next()
            if (extra !== first && extra !== targetNode) {
                try {
                    extra.recycle()
                } catch (_: Exception) {
                }
            }
        }
        return first
    }

    /**
     * 查询所有匹配的节点
     */
    private fun queryAllMatchedNodes(
        rootNode: AccessibilityNodeInfo,
        rule: ResolvedGKDRule
    ): List<AccessibilityNodeInfo> {
        val matchOption = MatchOption(fastQuery = rule.fastQuery ?: false)
        val nodes = mutableSetOf<AccessibilityNodeInfo>()

        // 添加 anyMatches 匹配的节点
        for (selector in rule.anyMatches) {
            nodes.addAll(transform.querySelectorAll(rootNode, selector, matchOption))
        }

        // 添加 matches 匹配的节点
        for (selector in rule.matches) {
            nodes.addAll(transform.querySelectorAll(rootNode, selector, matchOption))
        }

        return nodes.toList()
    }

    /**
     * 执行触发
     */
    private fun performTrigger(
        context: Context,
        state: GKDTriggerState,
        rule: ResolvedGKDRule,
        ruleState: RuleExecutionState,
        node: AccessibilityNodeInfo
    ) {
        if (!ruleState.shouldTrigger()) {
            return
        }

        // 更新状态
        ruleState.recordTrigger(System.currentTimeMillis())

        // 更新最后触发的规则（用于 preRules 检查）
        state.lastTriggerRule = rule

        // 创建 VScreenElement
        val element = VScreenElement.fromAccessibilityNode(node, calculateDepth(node))

        // 查询所有匹配的节点
        val allNodes = queryAllMatchedNodes(node, rule)
        val allElements = allNodes.map { VScreenElement.fromAccessibilityNode(it, calculateDepth(it)) }

        DebugLogger.d(
            TAG,
            "GKD订阅触发: 规则=${rule.name}, " +
                    "element=${element.asString()}, " +
                    "key=${rule.key}, " +
                    "actionCount=${ruleState.actionCount}"
        )

        executeTrigger(
            context = context,
            trigger = state.trigger,
            triggerData = GKDTriggerData(
                element = element,
                allElements = allElements,
                ruleName = rule.name,
                ruleGroup = rule.groupName
            )
        )
    }

    /**
     * 创建 Transform 实例
     */
    private fun createTransform(): Transform<AccessibilityNodeInfo> {
        return Transform(
            getAttr = { target, name ->
                when (target) {
                    is QueryContext<*> -> when (name) {
                        "prev" -> target.prev
                        "current" -> target.current
                        else -> getNodeAttr(target.current as AccessibilityNodeInfo, name)
                    }
                    is AccessibilityNodeInfo -> getNodeAttr(target, name)
                    else -> null
                }
            },
            getInvoke = getInvoke@{ target, name, args ->
                when (target) {
                    is QueryContext<*> -> getNodeInvoke(target.current as AccessibilityNodeInfo, name, args)
                    is AccessibilityNodeInfo -> getNodeInvoke(target, name, args)
                    else -> null
                }
            },
            getName = { node -> node.className?.toString() },
            getChildren = { node ->
                sequence {
                    for (i in 0 until node.childCount) {
                        node.getChild(i)?.let { yield(it) }
                    }
                }
            },
            getParent = { node -> node.parent },
            traverseFastQueryDescendants = { rootNode, fastQueryList ->
                fastQueryDescendants(rootNode, fastQueryList)
            }
        )
    }

    /**
     * 快速查询后代节点 - 使用 Android Accessibility API
     */
    private fun fastQueryDescendants(
        rootNode: AccessibilityNodeInfo,
        fastQueryList: List<FastQuery>
    ): Sequence<AccessibilityNodeInfo> {
        return sequence {
            for (fq in fastQueryList) {
                when (fq) {
                    is FastQuery.Text -> {
                        // 使用 findAccessibilityNodeInfosByText
                        val nodes = rootNode.findAccessibilityNodeInfosByText(fq.value)
                        for (node in nodes) {
                            // 验证节点是否是 rootNode 的后代
                            if (isDescendantOf(rootNode, node)) {
                                yield(node)
                            }
                        }
                    }
                    is FastQuery.Id -> {
                        // 使用 findAccessibilityNodeInfosByViewId
                        val nodes = rootNode.findAccessibilityNodeInfosByViewId(fq.value)
                        for (node in nodes) {
                            // 验证节点是否是 rootNode 的后代
                            if (isDescendantOf(rootNode, node)) {
                                yield(node)
                            }
                        }
                    }
                    is FastQuery.Vid -> {
                        // vid 需要拼接包名
                        val pkg = rootNode.packageName?.toString()
                        val vid = if (pkg != null && fq.value.startsWith(pkg)) {
                            fq.value
                        } else if (pkg != null) {
                            "$pkg:id/${fq.value}"
                        } else {
                            fq.value
                        }
                        val nodes = rootNode.findAccessibilityNodeInfosByViewId(vid)
                        DebugLogger.d(TAG, "fastQuery vid=$vid, 找到 ${nodes.size} 个节点")
                        for (node in nodes) {
                            // 验证节点是否是 rootNode 的后代
                            if (isDescendantOf(rootNode, node)) {
                                DebugLogger.d(TAG, "  ✓ 节点是后代，添加到候选: ${node.viewIdResourceName}")
                                yield(node)
                            } else {
                                DebugLogger.d(TAG, "  ✗ 节点不是后代，跳过: ${node.viewIdResourceName}")
                            }
                        }
                    }
                }
            }
        }
    }

    /**
     * 检查节点是否是另一个节点的后代
     */
    private fun isDescendantOf(ancestor: AccessibilityNodeInfo, node: AccessibilityNodeInfo): Boolean {
        if (ancestor == node) return false
        var current: AccessibilityNodeInfo? = node
        while (current != null) {
            val parent = current.parent
            if (parent == ancestor) return true
            // 防止无限循环（虽然理论上不应该发生）
            if (parent == node) break
            current = parent
        }
        return false
    }

    /**
     * 获取节点属性
     */
    private fun getNodeAttr(node: AccessibilityNodeInfo, name: String): Any? {
        return when (name) {
            "id" -> node.viewIdResourceName
            "vid" -> {
                // vid 处理：提取 viewIdResourceName 中的简短形式
                val id = node.viewIdResourceName ?: return null
                val pkg = node.packageName?.toString()
                if (pkg != null && id.startsWith(pkg) && id.contains(":id/")) {
                    id.substringAfterLast(":id/")
                } else {
                    id
                }
            }

            "name", "class", "className" -> node.className?.toString()
            "text" -> node.text?.toString()
            "desc", "contentDescription" -> node.contentDescription?.toString()

            "clickable" -> node.isClickable
            "focusable" -> node.isFocusable
            "checkable" -> node.isCheckable
            "checked" -> {
                // 使用 compatChecked 逻辑
                when (node.isChecked) {
                    true -> true
                    false -> false
                    else -> null  // CHECKED_STATE_PARTIAL
                }
            }

            "editable" -> node.isEditable
            "longClickable" -> node.isLongClickable
            "visibleToUser" -> node.isVisibleToUser
            "scrollable" -> node.isScrollable
            "selected" -> node.isSelected
            "focused" -> node.isFocused

            "childCount" -> node.childCount

            "index" -> getNodeIndex(node)

            else -> null
        }
    }

    /**
     * 获取节点在父节点中的索引
     */
    private fun getNodeIndex(node: AccessibilityNodeInfo): Int {
        val parent = node.parent ?: return -1
        for (i in 0 until parent.childCount) {
            if (parent.getChild(i) == node) {
                return i
            }
        }
        return -1
    }

    private fun getNodeInvoke(node: AccessibilityNodeInfo, name: String, args: List<Any>): Any? {
        return when (name) {
            "getChild" -> {
                val index = args.firstOrNull()?.toString()?.toIntOrNull() ?: 0
                if (index in 0 until node.childCount) node.getChild(index) else null
            }
            "getParent" -> node.parent
            else -> null
        }
    }

    private fun calculateDepth(node: AccessibilityNodeInfo): Int {
        var depth = 0
        var current: AccessibilityNodeInfo? = node
        while (current != null) {
            current = current.parent
            depth++
        }
        return depth
    }
}
