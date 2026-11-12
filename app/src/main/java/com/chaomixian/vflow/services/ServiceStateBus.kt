// 文件: main/java/com/chaomixian/vflow/services/ServiceStateBus.kt
package com.chaomixian.vflow.services

import android.content.Context
import android.content.Intent
import android.view.accessibility.AccessibilityEvent
import androidx.localbroadcastmanager.content.LocalBroadcastManager
import com.chaomixian.vflow.core.logging.DebugLogger
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.flow.asSharedFlow
import java.lang.ref.WeakReference

/**
 * 一个全局的服务状态总线，现在也负责分发无障碍事件。
 */
object ServiceStateBus {

    // 使用弱引用来持有服务实例，防止内存泄漏
    private var accessibilityServiceRef: WeakReference<AccessibilityService>? = null

    // 创建一个 SharedFlow 用于广播窗口变化事件 (包名, 类名)
    private val _windowChangeEventFlow = MutableSharedFlow<Pair<String, String>>()
    val windowChangeEventFlow = _windowChangeEventFlow.asSharedFlow()

    // 创建一个 SharedFlow 用于广播窗口内容变化事件 (包名, 类名)
    private val _windowContentChangedFlow = MutableSharedFlow<Pair<String, String>>()
    val windowContentChangedFlow = _windowContentChangedFlow.asSharedFlow()

    // ---- GKD 触发器专用事件通道 ----
    // 同一时刻只保留最新一条事件（含事件副本），避免 content-changed 高频事件把匹配队列撑爆；
    // 上面的 Pair 事件流保持原样，供其他触发器继续使用。
    class GkdWindowEvent(
        val packageName: String,
        val className: String,
        val event: AccessibilityEvent?,
    )

    // 小队列而不是“只留最新一条”：避免开屏这种极短状态被合并掉
    private const val GKD_EVENT_QUEUE_LIMIT = 8
    private val gkdEventQueue = ArrayDeque<GkdWindowEvent>()
    private val gkdEventLock = Any()

    private val _gkdEventSignal = MutableSharedFlow<Unit>(
        extraBufferCapacity = 1,
        onBufferOverflow = BufferOverflow.DROP_OLDEST,
    )
    val gkdEventSignal = _gkdEventSignal.asSharedFlow()

    /**
     * 由无障碍服务调用：投递一条 GKD 事件（内部复制 event，调用方无需关心生命周期）。
     */
    fun postGkdWindowEvent(packageName: String, className: String, event: AccessibilityEvent?) {
        // 没有 GKD 触发器在监听时不复制事件，避免给普通场景增加开销
        if (_gkdEventSignal.subscriptionCount.value == 0) return
        val copy = try {
            event?.let { AccessibilityEvent.obtain(it) }
        } catch (_: Exception) {
            null
        }
        val dropped = synchronized(gkdEventLock) {
            val overflow = if (gkdEventQueue.size >= GKD_EVENT_QUEUE_LIMIT) gkdEventQueue.removeFirst() else null
            gkdEventQueue.addLast(GkdWindowEvent(packageName, className, copy))
            overflow
        }
        dropped?.event?.recycle()
        _gkdEventSignal.tryEmit(Unit)
    }

    /**
     * 取出当前队列里的 GKD 事件；调用方用完必须 recycle 其中的 event。
     */
    fun takeGkdWindowEvents(): List<GkdWindowEvent> = synchronized(gkdEventLock) {
        if (gkdEventQueue.isEmpty()) return emptyList()
        val list = gkdEventQueue.toList()
        gkdEventQueue.clear()
        list
    }

    // 每个包最近一次窗口状态事件里的类名（Activity），避免 content-changed 的 View 类名把 Activity 顶掉
    private val activityClassByPackage = java.util.concurrent.ConcurrentHashMap<String, String>()

    fun rememberActivityClass(packageName: String, className: String) {
        activityClassByPackage[packageName] = className
    }

    fun activityClassFor(packageName: String): String? = activityClassByPackage[packageName]

    // 缓存最后一个窗口类名，供 UIInspector 使用
    var lastWindowClassName: String? = null
        private set

    // 缓存最后一个包名，供模块使用
    var lastWindowPackageName: String? = null
        private set

    // 缓存最近一次确认的 Activity 信息，供“所在页面”和获取当前 Activity 使用
    var lastActivityClassName: String? = null
        private set

    var lastActivityPackageName: String? = null
        private set

    // 定义广播动作
    const val ACTION_ACCESSIBILITY_SERVICE_STATE_CHANGED = "vflow.action.ACCESSIBILITY_SERVICE_STATE_CHANGED"
    const val EXTRA_IS_CONNECTED = "is_connected"

    /**
     * 当无障碍服务连接或断开时，由服务自身调用此方法来更新状态。
     */
    fun onAccessibilityServiceConnected(context: Context, service: AccessibilityService) {
        val previousRef = accessibilityServiceRef?.get()
        accessibilityServiceRef = WeakReference(service)
        DebugLogger.d(
            "ServiceStateBus",
            "无障碍服务状态更新: connected=true previous=${previousRef?.javaClass?.simpleName}@${previousRef?.let { Integer.toHexString(System.identityHashCode(it)) } ?: "null"} current=${service.javaClass.simpleName}@${Integer.toHexString(System.identityHashCode(service))}"
        )
        sendBroadcast(context, true)
    }

    fun onAccessibilityServiceDisconnected(context: Context) {
        val previousRef = accessibilityServiceRef?.get()
        accessibilityServiceRef?.clear()
        accessibilityServiceRef = null
        lastWindowClassName = null
        activityClassByPackage.clear()
        lastWindowPackageName = null
        lastActivityClassName = null
        lastActivityPackageName = null
        DebugLogger.w(
            "ServiceStateBus",
            "无障碍服务状态更新: connected=false previous=${previousRef?.javaClass?.simpleName}@${previousRef?.let { Integer.toHexString(System.identityHashCode(it)) } ?: "null"}"
        )
        sendBroadcast(context, false)
    }

    fun onAccessibilityServiceDisconnected(context: Context, service: AccessibilityService) {
        val currentService = accessibilityServiceRef?.get()
        if (currentService !== service) {
            DebugLogger.w(
                "ServiceStateBus",
                "忽略过期无障碍服务断开: stale=${service.javaClass?.simpleName}@${Integer.toHexString(System.identityHashCode(service))} current=${currentService?.javaClass?.simpleName}@${currentService?.let { Integer.toHexString(System.identityHashCode(it)) } ?: "null"}"
            )
            return
        }
        onAccessibilityServiceDisconnected(context)
    }

    /**
     * 由 AccessibilityService 调用，用于发出窗口变化事件。
     */
    suspend fun postWindowChangeEvent(
        packageName: String,
        className: String,
        confirmedActivityClassName: String? = null,
    ) {
        lastWindowClassName = className
        lastWindowPackageName = packageName
        if (!confirmedActivityClassName.isNullOrBlank()) {
            lastActivityClassName = confirmedActivityClassName
            lastActivityPackageName = packageName
        }
        rememberActivityClass(packageName, confirmedActivityClassName ?: className)
        _windowChangeEventFlow.emit(packageName to className)
    }

    /**
     * 由 AccessibilityService 调用，用于发出窗口内容变化事件。
     */
    suspend fun postWindowContentChanged(packageName: String, className: String) {
        lastWindowClassName = className
        lastWindowPackageName = packageName
        _windowContentChangedFlow.emit(packageName to className)
    }


    /**
     * 获取当前可用的无障碍服务实例。
     * 这是模块执行器获取服务的最可靠方式。
     */
    fun getAccessibilityService(): AccessibilityService? {
        return accessibilityServiceRef?.get()
    }

    /**
     * 检查无障碍服务当前是否正在运行。
     */
    fun isAccessibilityServiceRunning(): Boolean {
        return getAccessibilityService() != null
    }

    /**
     * 发送服务状态变更的本地广播。
     * UI层可以监听此广播来实时更新界面。
     */
    private fun sendBroadcast(context: Context, isConnected: Boolean) {
        val intent = Intent(ACTION_ACCESSIBILITY_SERVICE_STATE_CHANGED).apply {
            putExtra(EXTRA_IS_CONNECTED, isConnected)
        }
        LocalBroadcastManager.getInstance(context).sendBroadcast(intent)
    }
}
