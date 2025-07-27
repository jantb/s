package widgets

import ComponentOwn
import LogLevel
import app.KafkaLineDomain
import app.LogLineDomain
import org.junit.jupiter.api.Test
import util.UiColors
import java.awt.image.BufferedImage
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class ModernTextViewerTest {
    
    @Test
    fun `test JSON syntax highlighting`() {
        // Create a mock KafkaLineDomain with JSON content
        val jsonContent = """{"name": "John", "age": 30, "city": "New York", "active": true, "balance": null}"""
        val kafkaDomain = KafkaLineDomain(
            seq = 1L,
            level = LogLevel.INFO,
            timestamp = System.currentTimeMillis(),
            message = jsonContent,
            indexIdentifier = "test",
            topic = "test-topic",
            key = "test-key",
            offset = 0L,
            partition = 0,
            headers = "{}",
            compositeEventId = "test-event"
        )
        
        // Create ModernTextViewer instance
        val viewer = ModernTextViewer(
            parent = MockComponentOwn(),
            domain = kafkaDomain,
            x = 0, y = 0, width = 800, height = 600
        )
        
        // Test the highlightJson function directly
        val highlighted = viewer.highlightJson(jsonContent)
        
        // Verify that JSON elements are correctly highlighted
        // Check that we have the expected number of colored text segments
        assertTrue(highlighted.size > 0)
        
        // Check specific elements
        // First element should be opening brace (structural character - teal)
        assertEquals("{", highlighted[0].text)
        assertEquals(UiColors.teal, highlighted[0].color)
        
        // Check that we have string, colon, string, comma pattern for the first field
        assertTrue(highlighted.any { it.text == "\"name\"" && it.color == UiColors.green })
        assertTrue(highlighted.any { it.text == ":" && it.color == UiColors.teal })
        assertTrue(highlighted.any { it.text == "\"John\"" && it.color == UiColors.green })
        assertTrue(highlighted.any { it.text == "," && it.color == UiColors.teal })
        
        // Check number highlighting
        assertTrue(highlighted.any { it.text == "30" && it.color == UiColors.orange })
        
        // Check boolean highlighting
        assertTrue(highlighted.any { it.text == "true" && it.color == UiColors.magenta })
        
        // Check null highlighting
        assertTrue(highlighted.any { it.text == "null" && it.color == UiColors.magenta })
        
        // Last element should be closing brace (structural character - teal)
        assertEquals("}", highlighted.last().text)
        assertEquals(UiColors.teal, highlighted.last().color)
    }
    
    @Test
    fun `test stack trace syntax highlighting`() {
        // Create a mock LogLineDomain with stack trace content
        val stackTraceContent = """
            java.lang.NullPointerException: Something went wrong
                at com.example.MyClass.myMethod(MyClass.java:42)
                at com.example.AnotherClass.anotherMethod(AnotherClass.java:15)
        """.trimIndent()
        
        val logDomain = LogLineDomain(
            seq = 1L,
            level = LogLevel.ERROR,
            timestamp = System.currentTimeMillis(),
            message = "Error occurred",
            indexIdentifier = "test",
            threadName = "main",
            serviceName = "test-service",
            serviceVersion = "1.0",
            logger = "test-logger",
            stacktrace = stackTraceContent
        )
        
        // Create ModernTextViewer instance
        val viewer = ModernTextViewer(
            parent = MockComponentOwn(),
            domain = logDomain,
            x = 0, y = 0, width = 800, height = 600
        )
        
        // Test the highlightStackTrace function directly
        val highlighted = viewer.highlightStackTrace(stackTraceContent)
        
        // Verify that stack trace elements are correctly highlighted
        assertTrue(highlighted.size > 0)
        
        // Check exception highlighting (red)
        assertTrue(highlighted.any { it.text.contains("NullPointerException") && it.color == UiColors.red })
        
        // Check "at " keyword highlighting (teal)
        assertTrue(highlighted.any { it.text == "at " && it.color == UiColors.teal })
        
        // Check class/method highlighting (orange)
        assertTrue(highlighted.any { it.text == "com.example.MyClass.myMethod" && it.color == UiColors.orange })
        
        // Check file/line number highlighting (teal)
        assertTrue(highlighted.any { it.text == "(MyClass.java:42)" && it.color == UiColors.teal })
    }
    
    @Test
    fun `test highlightText function for JSON content`() {
        // Create a mock KafkaLineDomain with JSON content
        val jsonContent = """{"name": "John", "age": 30}"""
        val kafkaDomain = KafkaLineDomain(
            seq = 1L,
            level = LogLevel.INFO,
            timestamp = System.currentTimeMillis(),
            message = jsonContent,
            indexIdentifier = "test",
            topic = "test-topic",
            key = "test-key",
            offset = 0L,
            partition = 0,
            headers = "{}",
            compositeEventId = "test-event"
        )
        
        // Create ModernTextViewer instance
        val viewer = ModernTextViewer(
            parent = MockComponentOwn(),
            domain = kafkaDomain,
            x = 0, y = 0, width = 800, height = 600
        )
        
        // Test the highlightText function for JSON content in Message section
        val highlighted = viewer.highlightText(jsonContent, "Message")
        
        // Verify that JSON elements are correctly highlighted
        assertTrue(highlighted.size > 0)
        
        // Check that JSON content is properly highlighted
        assertTrue(highlighted.any { it.text == "{" && it.color == UiColors.teal })
        assertTrue(highlighted.any { it.text == "\"name\"" && it.color == UiColors.green })
        assertTrue(highlighted.any { it.text == "30" && it.color == UiColors.orange })
    }
}

// Simple mock class for testing
class MockComponentOwn : ComponentOwn() {
    override fun display(width: Int, height: Int, x: Int, y: Int): BufferedImage {
        return BufferedImage(width, height, BufferedImage.TYPE_INT_RGB)
    }
    
    override fun repaint(componentOwn: ComponentOwn) {
        // Do nothing
    }
    
    override fun keyTyped(e: java.awt.event.KeyEvent?) {}
    override fun keyPressed(e: java.awt.event.KeyEvent?) {}
    override fun keyReleased(e: java.awt.event.KeyEvent?) {}
    override fun mouseClicked(e: java.awt.event.MouseEvent?) {}
    override fun mousePressed(e: java.awt.event.MouseEvent?) {}
    override fun mouseReleased(e: java.awt.event.MouseEvent?) {}
    override fun mouseEntered(e: java.awt.event.MouseEvent?) {}
    override fun mouseExited(e: java.awt.event.MouseEvent?) {}
    override fun mouseWheelMoved(e: java.awt.event.MouseWheelEvent?) {}
    override fun mouseDragged(e: java.awt.event.MouseEvent?) {}
    override fun mouseMoved(e: java.awt.event.MouseEvent?) {}
}