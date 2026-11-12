package com.chaomixian.vflow.services

import android.Manifest
import android.content.BroadcastReceiver
import android.content.Context
import android.content.Intent
import android.content.pm.PackageManager
import android.provider.Telephony
import androidx.core.content.ContextCompat
import com.chaomixian.vflow.core.logging.DebugLogger

/**
 * 清单注册的短信接收器。
 *
 * 和原来的动态注册不同：即使 vFlow 进程/TriggerService 已经被系统回收，
 * 系统收到 SMS_RECEIVED 时也会拉起本进程来处理，不会因为服务不在而丢短信。
 */
class SmsReceiver : BroadcastReceiver() {

    companion object {
        private const val TAG = "SmsReceiver"
    }

    override fun onReceive(context: Context, intent: Intent) {
        if (intent.action != Telephony.Sms.Intents.SMS_RECEIVED_ACTION) return

        if (ContextCompat.checkSelfPermission(context, Manifest.permission.RECEIVE_SMS)
            != PackageManager.PERMISSION_GRANTED
        ) {
            DebugLogger.w(TAG, "收到短信但未授予 RECEIVE_SMS，忽略")
            return
        }

        val messages = Telephony.Sms.Intents.getMessagesFromIntent(intent)
        if (messages.isEmpty()) return

        val sender = messages.first().originatingAddress.orEmpty()
        val content = messages.joinToString("") { it.messageBody.orEmpty() }
        DebugLogger.d(TAG, "收到短信来自: $sender")
        SmsTriggerDispatcher.dispatch(context, sender, content)
    }
}
