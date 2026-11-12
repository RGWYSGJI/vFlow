package com.chaomixian.vflow.core.utils

import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Test

class CodeExtractorTest {

    @Test
    fun `code before keyword without space`() {
        assertEquals("123456", CodeExtractor.getCode("【某银行】123456是您的验证码，5分钟内有效。"))
    }

    @Test
    fun `code after keyword`() {
        assertEquals("123456", CodeExtractor.getCode("您的验证码为 123456，5分钟内有效"))
    }

    @Test
    fun `full width digits`() {
        assertEquals("123456", CodeExtractor.getCode("１２３４５６是您的验证码"))
    }

    @Test
    fun `letter prefix`() {
        assertEquals("G-123456", CodeExtractor.getCode("G-123456 是您的 Google 验证码"))
    }

    @Test
    fun `ignored version code`() {
        assertNull(CodeExtractor.getCode("versionCode 123456 是您的验证码"))
    }

    @Test
    fun `no keyword returns null`() {
        assertNull(CodeExtractor.getCode("订单号 12345678"))
    }

    @Test
    fun `date like number is ignored`() {
        assertNull(CodeExtractor.getCode("验证码 2024-12-31"))
    }
}
