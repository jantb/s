package widgets

import ComponentOwn
import app.DomainLine
import app.KafkaLineDomain
import app.LogLineDomain
import kotlinx.serialization.json.Json
import util.Styles
import util.UiColors
import java.awt.*
import java.awt.datatransfer.StringSelection
import java.awt.event.KeyEvent
import java.awt.event.MouseEvent
import java.awt.event.MouseWheelEvent
import java.awt.geom.Rectangle2D
import java.awt.image.BufferedImage
import kotlin.math.max
import kotlin.math.min

class ModernTextViewer(
    private val parent: ComponentOwn,
    private val domain: DomainLine,
    x: Int, y: Int, width: Int, height: Int
) : ComponentOwn() {
    
    private var image: BufferedImage
    private var g2d: Graphics2D
    private var fontSize = 12
    private var scrollY = 0
    private var maxScrollY = 0
    private var isVisible = false
    
    // Text selection
    private var selectionStart: TextPosition? = null
    private var selectionEnd: TextPosition? = null
    private var isDragging = false
    
    // Content sections
    private val sections = mutableListOf<TextSection>()
    private var contentHeight = 0
    
    private val json = Json { prettyPrint = true }
    
    data class TextPosition(val section: Int, val char: Int, val x: Int, val y: Int)
    data class TextSection(
        val label: String,
        val content: String,
        val isField: Boolean = true,
        val isExpandable: Boolean = false
    )
    
    data class ColoredText(
        val text: String,
        val color: Color
    )
    
    init {
        this.x = x
        this.y = y
        this.width = width
        this.height = height
        this.image = BufferedImage(width, height, BufferedImage.TYPE_INT_RGB)
        this.g2d = this.image.createGraphics()
        setupGraphics()
        buildContent()
    }
    
    private fun setupGraphics() {
        g2d.setRenderingHint(RenderingHints.KEY_TEXT_ANTIALIASING, RenderingHints.VALUE_TEXT_ANTIALIAS_ON)
        g2d.font = Font(Styles.normalFont, Font.PLAIN, fontSize)
    }
    
    private fun buildContent() {
        sections.clear()
        
        when (domain) {
            is KafkaLineDomain -> {
                sections.add(TextSection("Timestamp", kotlinx.datetime.Instant.fromEpochMilliseconds(domain.timestamp).toString()))
                domain.correlationId?.let { sections.add(TextSection("CorrelationId", it)) }
                domain.requestId?.let { sections.add(TextSection("RequestId", it)) }
                sections.add(TextSection("Headers", domain.headers))
                sections.add(TextSection("Topic", domain.topic))
                sections.add(TextSection("Partition", domain.partition.toString()))
                sections.add(TextSection("Key", domain.key.toString()))
                sections.add(TextSection("Offset", domain.offset.toString()))
                sections.add(TextSection("Composite Event ID", domain.compositeEventId))
                
                val formattedMessage = try {
                    json.encodeToString(Json.parseToJsonElement(domain.message))
                } catch (e: Exception) {
                    domain.message
                }
                sections.add(TextSection("Message", formattedMessage, isField = false))
            }
            
            is LogLineDomain -> {
                sections.add(TextSection("Timestamp", kotlinx.datetime.Instant.fromEpochMilliseconds(domain.timestamp).toString()))
                domain.correlationId?.let { sections.add(TextSection("CorrelationId", it)) }
                domain.requestId?.let { sections.add(TextSection("RequestId", it)) }
                domain.errorMessage?.let { sections.add(TextSection("Error Message", it)) }
                sections.add(TextSection("Level", domain.level.name))
                sections.add(TextSection("Application", "${domain.serviceName}:${domain.serviceVersion}"))
                sections.add(TextSection("Logger", domain.logger))
                sections.add(TextSection("Thread", domain.threadName))
                
                val fullMessage = buildString {
                    append(domain.message)
                    domain.errorMessage?.let { append(" ").append(it) }
                    domain.stacktrace?.let { append("\n").append(it) }
                }
                sections.add(TextSection("Message", fullMessage, isField = false))
            }
        }
        
        calculateContentHeight()
    }
    
    private fun calculateContentHeight() {
        val metrics = g2d.fontMetrics
        val lineHeight = metrics.height + 4
        val fieldHeight = lineHeight + 8
        
        contentHeight = 40 // Header space
        
        for (section in sections) {
            contentHeight += if (section.isField) {
                fieldHeight
            } else {
                val wrappedLines = wrapText(section.content, width - 40)
                wrappedLines.size * lineHeight + 20
            }
        }
        
        maxScrollY = max(0, contentHeight - height + 60)
    }
    
    private fun wrapText(text: String, maxWidth: Int): List<String> {
        val metrics = g2d.fontMetrics
        val lines = mutableListOf<String>()
        
        text.split('\n').forEach { line ->
            if (metrics.stringWidth(line) <= maxWidth) {
                lines.add(line)
            } else {
                val words = line.split(' ')
                var currentLine = ""
                
                for (word in words) {
                    val testLine = if (currentLine.isEmpty()) word else "$currentLine $word"
                    if (metrics.stringWidth(testLine) <= maxWidth) {
                        currentLine = testLine
                    } else {
                        if (currentLine.isNotEmpty()) {
                            lines.add(currentLine)
                            currentLine = word
                        } else {
                            // Word is too long, break it
                            var remaining = word
                            while (remaining.isNotEmpty()) {
                                var fit = ""
                                for (i in remaining.indices) {
                                    val test = remaining.substring(0, i + 1)
                                    if (metrics.stringWidth(test) <= maxWidth) {
                                        fit = test
                                    } else {
                                        break
                                    }
                                }
                                if (fit.isEmpty()) fit = remaining.substring(0, 1)
                                lines.add(fit)
                                remaining = remaining.substring(fit.length)
                            }
                        }
                    }
                }
                if (currentLine.isNotEmpty()) {
                    lines.add(currentLine)
                }
            }
        }
        
        return lines
    }
    
    fun highlightJson(text: String): List<ColoredText> {
        val result = mutableListOf<ColoredText>()
        var i = 0
        
        while (i < text.length) {
            val char = text[i]
            when (char) {
                '{', '}', '[', ']', ':', ',' -> {
                    // JSON structural characters
                    result.add(ColoredText(char.toString(), UiColors.teal))
                    i++
                }
                '"' -> {
                    // String literals
                    val start = i
                    i++ // Skip opening quote
                    var escaped = false
                    while (i < text.length && (text[i] != '"' || escaped)) {
                        if (text[i] == '\\' && !escaped) {
                            escaped = true
                        } else {
                            escaped = false
                        }
                        i++
                    }
                    if (i < text.length) i++ // Skip closing quote
                    result.add(ColoredText(text.substring(start, i), UiColors.green))
                }
                '-', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9' -> {
                    // Numbers
                    val start = i
                    while (i < text.length && (text[i].isDigit() || text[i] == '.' || text[i] == 'e' || text[i] == 'E' || text[i] == '+' || text[i] == '-')) {
                        i++
                    }
                    result.add(ColoredText(text.substring(start, i), UiColors.orange))
                }
                't', 'f', 'n' -> {
                    // Boolean and null literals
                    if (i + 3 < text.length && text.substring(i, i + 4) == "true") {
                        result.add(ColoredText("true", UiColors.magenta))
                        i += 4
                    } else if (i + 4 < text.length && text.substring(i, i + 5) == "false") {
                        result.add(ColoredText("false", UiColors.magenta))
                        i += 5
                    } else if (i + 3 < text.length && text.substring(i, i + 4) == "null") {
                        result.add(ColoredText("null", UiColors.magenta))
                        i += 4
                    } else {
                        result.add(ColoredText(char.toString(), UiColors.defaultText))
                        i++
                    }
                }
                ' ', '\t', '\n', '\r' -> {
                    // Whitespace
                    val start = i
                    while (i < text.length && text[i].isWhitespace()) {
                        i++
                    }
                    result.add(ColoredText(text.substring(start, i), UiColors.defaultText))
                }
                else -> {
                    // Regular text
                    result.add(ColoredText(char.toString(), UiColors.defaultText))
                    i++
                }
            }
        }
        
        return result
    }
    
    fun highlightStackTrace(text: String): List<ColoredText> {
        val result = mutableListOf<ColoredText>()
        val lines = text.split('\n')
        
        for (line in lines) {
            when {
                line.startsWith("at ") -> {
                    // Stack trace element
                    val parenIndex = line.indexOf('(')
                    if (parenIndex != -1) {
                        // Highlight "at " keyword
                        result.add(ColoredText("at ", UiColors.teal))
                        // Highlight class and method
                        result.add(ColoredText(line.substring(3, parenIndex), UiColors.orange))
                        // Highlight file and line number
                        result.add(ColoredText(line.substring(parenIndex), UiColors.teal))
                    } else {
                        result.add(ColoredText("at ", UiColors.teal))
                        result.add(ColoredText(line.substring(3), UiColors.orange))
                    }
                }
                line.contains("Exception") || line.contains("Error") -> {
                    // Exception class names
                    result.add(ColoredText(line, UiColors.red))
                }
                line.startsWith("\tat ") -> {
                    // Tabbed stack trace element
                    result.add(ColoredText("\tat ", UiColors.teal))
                    val parenIndex = line.indexOf('(', 2)
                    if (parenIndex != -1) {
                        result.add(ColoredText(line.substring(4, parenIndex), UiColors.orange))
                        result.add(ColoredText(line.substring(parenIndex), UiColors.teal))
                    } else {
                        result.add(ColoredText(line.substring(4), UiColors.orange))
                    }
                }
                else -> {
                    result.add(ColoredText(line, UiColors.defaultText))
                }
            }
            result.add(ColoredText("\n", UiColors.defaultText))
        }
        
        // Remove the last newline if it exists
        if (result.isNotEmpty() && result.last().text == "\n") {
            result.removeAt(result.size - 1)
        }
        
        return result
    }
    
    fun highlightText(content: String, sectionLabel: String): List<ColoredText> {
        return when {
            sectionLabel == "Message" && (domain is LogLineDomain || domain is KafkaLineDomain) -> {
                // Check if content looks like JSON
                val trimmedContent = content.trimStart()
                if (trimmedContent.startsWith("{") || trimmedContent.startsWith("[")) {
                    highlightJson(content)
                } else if (domain is LogLineDomain && domain.stacktrace != null &&
                           content.contains("\n") &&
                           (content.contains("Exception") || content.contains("Error") || content.contains("at "))) {
                    // This is likely a message with embedded stack trace
                    highlightStackTrace(content)
                } else if (domain is LogLineDomain && domain.stacktrace != null) {
                    // Separate stack trace handling
                    val fullContent = buildString {
                        append(content)
                        domain.stacktrace?.let { append("\n").append(it) }
                    }
                    highlightStackTrace(fullContent)
                } else {
                    listOf(ColoredText(content, UiColors.defaultText))
                }
            }
            sectionLabel == "Stacktrace" && domain is LogLineDomain -> {
                highlightStackTrace(content)
            }
            else -> {
                listOf(ColoredText(content, UiColors.defaultText))
            }
        }
    }
    
    fun show() {
        isVisible = true
        parent.repaint(this)
    }
    
    fun hide() {
        isVisible = false
        parent.repaint(this)
    }
    
    override fun display(width: Int, height: Int, x: Int, y: Int): BufferedImage {
        if (!isVisible) return image
        
        if (this.width != width || this.height != height) {
            this.width = width
            this.height = height
            this.image = BufferedImage(width, height, BufferedImage.TYPE_INT_RGB)
            this.g2d = this.image.createGraphics()
            setupGraphics()
            calculateContentHeight()
        }
        
        // Clear background
        g2d.color = UiColors.background
        g2d.fillRect(0, 0, width, height)
        
        // Draw border
        g2d.color = UiColors.selectionLine
        g2d.drawRect(0, 0, width - 1, height - 1)
        
        // Draw title bar
        g2d.color = UiColors.backgroundTeal
        g2d.fillRect(1, 1, width - 2, 30)
        g2d.color = UiColors.defaultText
        g2d.drawString("Log Details - ${if (domain is KafkaLineDomain) "Kafka" else "Log"}", 10, 20)
        
        // Draw close button
        g2d.color = UiColors.red
        g2d.fillRect(width - 25, 5, 20, 20)
        g2d.color = UiColors.defaultText
        g2d.drawString("×", width - 18, 18)
        
        // Draw content
        drawContent()
        
        // Draw scrollbar if needed
        if (maxScrollY > 0) {
            drawScrollbar()
        }
        
        return image
    }
    
    private fun drawContent() {
        val metrics = g2d.fontMetrics
        val lineHeight = metrics.height + 4
        val fieldHeight = lineHeight + 8
        var currentY = 40 - scrollY
        
        g2d.color = UiColors.defaultText
        
        for ((index, section) in sections.withIndex()) {
            if (currentY > height) break
            if (currentY + (if (section.isField) fieldHeight else lineHeight * 3) < 0) {
                currentY += if (section.isField) fieldHeight else {
                    val wrappedLines = wrapText(section.content, width - 40)
                    wrappedLines.size * lineHeight + 20
                }
                continue
            }
            
            if (section.isField) {
                // Draw field label
                g2d.color = UiColors.teal
                g2d.drawString("${section.label}:", 10, currentY + 15)
                
                // Draw copy button
                g2d.color = UiColors.selectionLine
                g2d.fillRect(width - 60, currentY + 2, 50, 20)
                g2d.color = UiColors.defaultText
                g2d.drawString("Copy", width - 50, currentY + 15)
                
                // Draw field content
                g2d.color = UiColors.defaultText
                val fieldContent = if (section.content.length > 80) {
                    section.content.take(80) + "..."
                } else {
                    section.content
                }
                g2d.drawString(fieldContent, 10, currentY + 35)
                
                currentY += fieldHeight
            } else {
                // Draw section label
                g2d.color = UiColors.teal
                g2d.drawString("${section.label}:", 10, currentY + 15)
                currentY += 25
                
                // Draw wrapped content with syntax highlighting
                g2d.color = UiColors.defaultText
                val coloredTexts = highlightText(section.content, section.label)
                drawColoredText(coloredTexts, 20, currentY, width - 40, lineHeight)
                val wrappedLines = wrapText(section.content, width - 40)
                currentY += wrappedLines.size * lineHeight + 10
            }
        }
    }
    
    private fun drawColoredText(coloredTexts: List<ColoredText>, x: Int, startY: Int, maxWidth: Int, lineHeight: Int) {
        var currentX = x
        var currentY = startY
        val metrics = g2d.fontMetrics
        
        for (coloredText in coloredTexts) {
            g2d.color = coloredText.color
            val text = coloredText.text
            
            // Handle multi-line text
            val lines = text.split('\n')
            for (i in lines.indices) {
                val line = lines[i]
                if (i > 0) {
                    // New line
                    currentY += lineHeight
                    currentX = x
                }
                
                if (line.isEmpty()) continue
                
                // Simple approach: just draw the text without complex wrapping for now
                if (currentY > 0 && currentY < height) {
                    g2d.drawString(line, currentX, currentY)
                }
                currentX += metrics.stringWidth(line)
            }
        }
    }
    
    private fun drawScrollbar() {
        val scrollbarWidth = 10
        val scrollbarX = width - scrollbarWidth - 2
        val scrollbarHeight = height - 40
        val thumbHeight = max(20, (scrollbarHeight * height) / contentHeight)
        val thumbY = 32 + (scrollY * (scrollbarHeight - thumbHeight)) / maxScrollY
        
        // Draw scrollbar track
        g2d.color = UiColors.selectionLine
        g2d.fillRect(scrollbarX, 32, scrollbarWidth, scrollbarHeight)
        
        // Draw scrollbar thumb
        g2d.color = UiColors.defaultText
        g2d.fillRect(scrollbarX, thumbY, scrollbarWidth, thumbHeight)
    }
    
    private fun copyToClipboard(text: String) {
        val stringSelection = StringSelection(text)
        val clipboard = Toolkit.getDefaultToolkit().systemClipboard
        clipboard.setContents(stringSelection, null)
    }
    
    private fun getSelectedText(): String {
        val start = selectionStart ?: return ""
        val end = selectionEnd ?: return ""
        
        if (start.section == end.section) {
            val section = sections[start.section]
            val startChar = min(start.char, end.char)
            val endChar = max(start.char, end.char)
            return section.content.substring(startChar, endChar)
        }
        
        // Multi-section selection
        val result = StringBuilder()
        for (i in start.section..end.section) {
            val section = sections[i]
            when (i) {
                start.section -> result.append(section.content.substring(start.char))
                end.section -> result.append(section.content.substring(0, end.char))
                else -> result.append(section.content)
            }
            if (i < end.section) result.append("\n")
        }
        return result.toString()
    }
    
    override fun repaint(componentOwn: ComponentOwn) {
        parent.repaint(this)
    }
    
    override fun keyTyped(e: KeyEvent?) {}
    override fun keyPressed(e: KeyEvent?) {
        e?.let {
            when (it.keyCode) {
                KeyEvent.VK_ESCAPE -> hide()
                KeyEvent.VK_C -> if (it.isControlDown || it.isMetaDown) {
                    val selectedText = getSelectedText()
                    if (selectedText.isNotEmpty()) {
                        copyToClipboard(selectedText)
                    }
                }
            }
        }
    }
    override fun keyReleased(e: KeyEvent?) {}
    
    override fun mouseClicked(e: MouseEvent) {
        // Check close button
        if (e.x >= width - 25 && e.x <= width - 5 && e.y >= 5 && e.y <= 25) {
            hide()
            return
        }
        
        // Check copy buttons
        if (e.x >= width - 60 && e.x <= width - 10) {
            var currentY = 40 - scrollY
            val fieldHeight = g2d.fontMetrics.height + 12
            
            for (section in sections) {
                if (section.isField && e.y >= currentY + 2 && e.y <= currentY + 22) {
                    copyToClipboard(section.content)
                    return
                }
                currentY += if (section.isField) fieldHeight else {
                    val wrappedLines = wrapText(section.content, width - 40)
                    wrappedLines.size * (g2d.fontMetrics.height + 4) + 20
                }
            }
        }
    }
    
    override fun mousePressed(e: MouseEvent?) {
        e?.let {
            isDragging = true
            // Start text selection
        }
    }
    
    override fun mouseReleased(e: MouseEvent?) {
        isDragging = false
    }
    
    override fun mouseEntered(e: MouseEvent) {}
    override fun mouseExited(e: MouseEvent) {}
    
    override fun mouseWheelMoved(e: MouseWheelEvent?) {
        e?.let {
            if (it.isControlDown || it.isMetaDown) {
                // Font size adjustment
                fontSize += -it.wheelRotation
                fontSize = fontSize.coerceIn(8, 24)
                g2d.font = Font(Styles.normalFont, Font.PLAIN, fontSize)
                calculateContentHeight()
                parent.repaint(this)
            } else {
                // Scrolling
                scrollY += it.wheelRotation * 20
                scrollY = scrollY.coerceIn(0, maxScrollY)
                parent.repaint(this)
            }
        }
    }
    
    override fun mouseDragged(e: MouseEvent?) {
        // Handle text selection dragging
    }
    
    override fun mouseMoved(e: MouseEvent) {}
}