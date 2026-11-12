// 文件: main/java/com/chaomixian/vflow/core/workflow/module/triggers/handlers/SmsTriggerHandler.kt
// 描述: 短信触发器处理器。SMS_RECEIVED 由清单注册的 SmsReceiver 接收，
//       本类只负责“匹配规则 + 执行工作流”，不再自己动态注册广播。
package com.chaomixian.vflow.core.workflow.module.triggers.handlers

import android.content.Context
import com.chaomixian.vflow.core.logging.DebugLogger
import com.chaomixian.vflow.core.types.basic.VDictionary
import com.chaomixian.vflow.core.types.basic.VString
import com.chaomixian.vflow.core.utils.CodeExtractor
import com.chaomixian.vflow.core.workflow.TriggerExecutionCoordinator
import com.chaomixian.vflow.core.workflow.model.TriggerSpec
import com.chaomixian.vflow.core.workflow.module.triggers.SmsTriggerModule
import com.chaomixian.vflow.services.SmsTriggerDispatcher
import kotlinx.coroutines.launch
import java.util.regex.Pattern

class SmsTriggerHandler : ListeningTriggerHandler() {

    companion object {
        private const val TAG = "SmsTriggerHandler"

        private val smsTriggerModuleId: String by lazy { SmsTriggerModule().id }

        fun isSmsTrigger(trigger: TriggerSpec): Boolean = trigger.type == smsTriggerModuleId

        /**
         * 处理一条短信：按触发器配置过滤、提取验证码并执行工作流。
         * 静态方法，TriggerService 不在时也能由 SmsTriggerDispatcher 直接调用。
         */
        suspend fun processSms(
            context: Context,
            trigger: TriggerSpec,
            sender: String,
            content: String,
        ) {
            val matchResult = checkFilters(sender, content, trigger.parameters)
            if (!matchResult.isMatch) return

            DebugLogger.i(TAG, "短信满足条件，触发工作流 '${trigger.workflowName}'")
            val triggerDataMap = mutableMapOf(
                "sender" to VString(sender),
                "content" to VString(content)
            )
            matchResult.verificationCode?.let {
                triggerDataMap["verification_code"] = VString(it)
            }
            TriggerExecutionCoordinator.executeTrigger(
                context = context,
                trigger = trigger,
                triggerData = VDictionary(triggerDataMap)
            )
        }

        private data class FilterMatchResult(val isMatch: Boolean, val verificationCode: String? = null)

        /**
         * 检查发件人/内容过滤条件，命中“识别验证码”时返回提取到的验证码。
         */
        private fun checkFilters(
            sender: String,
            content: String,
            config: Map<String, Any?>
        ): FilterMatchResult {
            val inputsById = SmsTriggerModule().getInputs().associateBy { it.id }
            val rawSenderFilterType = config["sender_filter_type"] as? String ?: SmsTriggerModule.SENDER_ANY
            val senderFilterType = inputsById["sender_filter_type"]?.normalizeEnumValue(rawSenderFilterType)
                ?: rawSenderFilterType
            val senderFilterValue = config["sender_filter_value"] as? String ?: ""
            val rawContentFilterType = config["content_filter_type"] as? String ?: SmsTriggerModule.CONTENT_ANY
            val contentFilterType = inputsById["content_filter_type"]?.normalizeEnumValue(rawContentFilterType)
                ?: rawContentFilterType
            val contentFilterValue = config["content_filter_value"] as? String ?: ""

            val senderMatches = when (senderFilterType) {
                SmsTriggerModule.SENDER_ANY -> true
                SmsTriggerModule.SENDER_CONTAINS -> sender.contains(senderFilterValue)
                SmsTriggerModule.SENDER_NOT_CONTAINS -> !sender.contains(senderFilterValue)
                SmsTriggerModule.SENDER_REGEX -> try {
                    Pattern.compile(senderFilterValue).matcher(sender).find()
                } catch (e: Exception) {
                    false
                }
                else -> false
            }
            if (!senderMatches) return FilterMatchResult(false)

            if (contentFilterType == SmsTriggerModule.CONTENT_CODE) {
                val code = CodeExtractor.getCode(content)
                return if (code != null) FilterMatchResult(true, code) else FilterMatchResult(false)
            }

            val contentMatches = when (contentFilterType) {
                SmsTriggerModule.CONTENT_ANY -> true
                SmsTriggerModule.CONTENT_CONTAINS -> content.contains(contentFilterValue, ignoreCase = true)
                SmsTriggerModule.CONTENT_NOT_CONTAINS -> !content.contains(contentFilterValue, ignoreCase = true)
                SmsTriggerModule.CONTENT_REGEX -> try {
                    Pattern.compile(contentFilterValue).matcher(content).find()
                } catch (e: Exception) {
                    false
                }
                else -> false
            }
            return FilterMatchResult(contentMatches)
        }
    }

    override fun startListening(context: Context) {
        // 不再动态注册 receiver：SMS_RECEIVED 由清单里的 SmsReceiver 接收。
        // 这里只把自己挂到派发器上，服务活着时优先使用当前已加载的触发器列表。
        SmsTriggerDispatcher.attach(this)
        DebugLogger.d(TAG, "短信监听已挂载（清单 receiver 模式）")
    }

    override fun stopListening(context: Context) {
        SmsTriggerDispatcher.detach(this)
        DebugLogger.d(TAG, "短信监听已停止。")
    }

    /** TriggerService 活着时，由 SmsTriggerDispatcher 调用。 */
    fun handleSms(context: Context, sender: String, content: String) {
        val appContext = context.applicationContext
        triggerScope.launch {
            listeningTriggers.forEach { trigger ->
                try {
                    processSms(appContext, trigger, sender, content)
                } catch (e: Exception) {
                    DebugLogger.e(TAG, "处理短信失败: ${trigger.workflowName}", e)
                }
            }
        }
    }
}
