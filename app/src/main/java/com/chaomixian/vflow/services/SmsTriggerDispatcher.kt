package com.chaomixian.vflow.services

import android.content.Context
import android.content.Intent
import com.chaomixian.vflow.core.logging.DebugLogger
import com.chaomixian.vflow.core.module.ModuleRegistry
import com.chaomixian.vflow.core.workflow.WorkflowManager
import com.chaomixian.vflow.core.workflow.module.scripted.ModuleManager
import androidx.core.content.ContextCompat
import com.chaomixian.vflow.core.workflow.module.triggers.handlers.SmsTriggerHandler
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch

/**
 * 短信事件派发器：连接清单 SmsReceiver 和触发引擎。
 *
 * - TriggerService 活着：交给它已经加载好的 SmsTriggerHandler（只处理当前启用的触发器）；
 * - TriggerService 没活着：直接从 WorkflowManager 读取启用的短信触发器并执行，
 *   不依赖前台服务，保证“服务被杀也能触发”。
 */
object SmsTriggerDispatcher {

    private const val TAG = "SmsTriggerDispatcher"
    private val scope = CoroutineScope(Dispatchers.IO + SupervisorJob())

    @Volatile
    private var activeHandler: SmsTriggerHandler? = null

    fun attach(handler: SmsTriggerHandler) {
        activeHandler = handler
    }

    fun detach(handler: SmsTriggerHandler) {
        if (activeHandler === handler) activeHandler = null
    }

    fun dispatch(context: Context, sender: String, content: String) {
        val handler = activeHandler
        if (handler != null) {
            handler.handleSms(context, sender, content)
            return
        }

        // 服务没活着：优先把短信交给 TriggerService（前台服务），保证工作流执行期间进程不被回收。
        try {
            val intent = Intent(context, TriggerService::class.java).apply {
                action = TriggerService.ACTION_SMS_RECEIVED
                putExtra(TriggerService.EXTRA_SMS_SENDER, sender)
                putExtra(TriggerService.EXTRA_SMS_CONTENT, content)
            }
            ContextCompat.startForegroundService(context, intent)
            DebugLogger.d(TAG, "TriggerService 未运行，已请求其处理短信")
            return
        } catch (e: Exception) {
            DebugLogger.w(TAG, "拉起 TriggerService 失败，改为直接处理短信", e)
        }

        val appContext = context.applicationContext
        scope.launch {
            try {
                // 冷启动时进程可能只被 SmsReceiver 拉起，Application 没有初始化模块；
                // 直接处理短信前补齐，避免 WorkflowExecutor 找不到模块。
                ModuleRegistry.initialize(appContext)
                ModuleManager.loadModules(appContext)

                val triggers = WorkflowManager(appContext).getAllWorkflows()
                    .filter { it.isEnabled }
                    .flatMap { it.toAutoTriggerSpecs() }
                    .filter { SmsTriggerHandler.isSmsTrigger(it) }
                if (triggers.isEmpty()) {
                    DebugLogger.d(TAG, "没有启用的短信触发器")
                    return@launch
                }
                triggers.forEach { trigger ->
                    SmsTriggerHandler.processSms(appContext, trigger, sender, content)
                }
            } catch (e: Exception) {
                DebugLogger.e(TAG, "直接处理短信失败", e)
            }
        }
    }
}
