package widgets

import app.DomainLine
import app.KafkaLineDomain
import app.LogLineDomain
import kotlinx.serialization.json.Json
import util.Styles
import util.UiColors
import java.awt.*
import java.awt.datatransfer.StringSelection
import java.awt.event.*
import javax.swing.*
import javax.swing.border.EmptyBorder
import javax.swing.text.*

class ModernTextViewerWindow(title: String = "Log Details", private val domain: DomainLine) : JFrame() {

    private val json = Json { prettyPrint = true }

    data class ColoredText(
        val text: String,
        val color: Color
    )

    fun highlightJson(text: String): List<ColoredText> {
        val result = mutableListOf<ColoredText>()
        var i = 0
        var expectKey = true // Flag to track if we're expecting a key

        while (i < text.length) {
            val char = text[i]
            when (char) {
                '{', '[', ',' -> {
                    // JSON structural characters that indicate we're expecting a key next
                    if (char == '{' || char == '[' || char == ',') {
                        expectKey = true
                    }
                    result.add(ColoredText(char.toString(), UiColors.teal))
                    i++
                }

                '}', ']' -> {
                    // Closing brackets
                    result.add(ColoredText(char.toString(), UiColors.teal))
                    i++
                }

                ':' -> {
                    // Colon separates key from value
                    result.add(ColoredText(char.toString(), UiColors.teal))
                    expectKey = false // After colon, we expect a value
                    i++
                }

                '"' -> {
                    // String literals (keys or values)
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

                    // Color the string based on whether it's a key or value
                    val color = if (expectKey) UiColors.magenta else UiColors.green
                    result.add(ColoredText(text.substring(start, i), color))
                }

                '-', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9' -> {
                    // Numbers (always values)
                    val start = i
                    while (i < text.length && (text[i].isDigit() || text[i] == '.' || text[i] == 'e' || text[i] == 'E' || text[i] == '+' || text[i] == '-')) {
                        i++
                    }
                    result.add(ColoredText(text.substring(start, i), UiColors.orange))
                }

                't', 'f', 'n' -> {
                    // Boolean and null literals (always values)
                    if (i + 3 < text.length && text.substring(i, i + 4) == "true") {
                        result.add(ColoredText("true", UiColors.orange))
                        i += 4
                    } else if (i + 4 < text.length && text.substring(i, i + 5) == "false") {
                        result.add(ColoredText("false", UiColors.orange))
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
                    (content.contains("Exception") || content.contains("Error") || content.contains("at "))
                ) {
                    // This is likely a message with embedded stack trace
                    highlightStackTrace(content)
                } else if (domain is LogLineDomain && domain.stacktrace != null) {
                    // Separate stack trace handling
                    val fullContent = buildString {
                        append(content)
                        domain.stacktrace.let { append("\n").append(it) }
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

    private fun setStyledText(textPane: JTextPane, content: String) {
        val doc = DefaultStyledDocument()
        val coloredTexts = highlightText(content, "Message")

        try {
            var offset = 0
            for (coloredText in coloredTexts) {
                val style = SimpleAttributeSet()
                StyleConstants.setForeground(style, coloredText.color)
                StyleConstants.setFontFamily(style, Styles.monoFont)
                StyleConstants.setFontSize(style, 12)
                doc.insertString(offset, coloredText.text, style)
                offset += coloredText.text.length
            }
        } catch (e: BadLocationException) {
            e.printStackTrace()
        }

        // Set the document to the text pane
        textPane.styledDocument = doc
    }

    init {
        setupWindow(title)
        createContent()
        setupEventHandlers()
    }

    private fun setupWindow(title: String) {
        setTitle(title)
        setSize(900, 700)
        defaultCloseOperation = DISPOSE_ON_CLOSE

        // Center on screen
        val mouseLocation = MouseInfo.getPointerInfo().location
        setLocation(mouseLocation.x - width / 2, mouseLocation.y - height / 2)

        // Modern dark theme
        background = UiColors.background

        // Set window icon if available
        try {
            iconImage = ImageIcon(ClassLoader.getSystemResource("logo.png")).image
        } catch (e: Exception) {
            // Ignore if icon not found
        }
    }

    private fun createContent() {
        layout = BorderLayout()

        // Create main panel with dark background
        val mainPanel = JPanel(BorderLayout()).apply {
            background = UiColors.background
            border = EmptyBorder(10, 10, 10, 10)
        }

        // Create header panel
        val headerPanel = createHeaderPanel()
        mainPanel.add(headerPanel, BorderLayout.NORTH)

        // Create content panel with fields and message
        val contentPanel = createContentPanel()
        mainPanel.add(contentPanel, BorderLayout.CENTER)

        add(mainPanel, BorderLayout.CENTER)
    }

    private fun createHeaderPanel(): JPanel {
        val headerPanel = JPanel(BorderLayout()).apply {
            background = UiColors.backgroundTeal
            border = EmptyBorder(6, 12, 6, 12)
            preferredSize = Dimension(0, 32)
        }

        // Title label
        val titleLabel = JLabel("${if (domain is KafkaLineDomain) "Kafka" else "Log"} Line Details").apply {
            foreground = UiColors.defaultText
            font = Font(Styles.normalFont, Font.BOLD, 12)
        }
        headerPanel.add(titleLabel, BorderLayout.CENTER)

        return headerPanel
    }

    private fun createContentPanel(): JPanel {
        val contentPanel = JPanel(BorderLayout()).apply {
            background = UiColors.background
        }

        // Create fields panel
        val fieldsPanel = createFieldsPanel()
        contentPanel.add(fieldsPanel, BorderLayout.NORTH)

        // Create message panel
        val messagePanel = createMessagePanel()
        contentPanel.add(messagePanel, BorderLayout.CENTER)

        return contentPanel
    }

    private fun createFieldsPanel(): JPanel {
        val fieldsPanel = JPanel().apply {
            layout = BoxLayout(this, BoxLayout.Y_AXIS)
            background = UiColors.background
            border = EmptyBorder(6, 0, 6, 0)
        }

        when (domain) {
            is KafkaLineDomain -> {
                addField(
                    fieldsPanel,
                    "Timestamp",
                    kotlinx.datetime.Instant.fromEpochMilliseconds(domain.timestamp).toString()
                )
                domain.correlationId?.let { addField(fieldsPanel, "Correlation ID", it) }
                domain.requestId?.let { addField(fieldsPanel, "Request ID", it) }
                addField(fieldsPanel, "Topic", domain.topic)
                addField(fieldsPanel, "Partition", domain.partition.toString())
                addField(fieldsPanel, "Offset", domain.offset.toString())
                addField(fieldsPanel, "Key", domain.key ?: "null")
                addField(fieldsPanel, "Composite Event ID", domain.compositeEventId)
                addField(fieldsPanel, "Headers", domain.headers)
            }

            is LogLineDomain -> {
                addField(
                    fieldsPanel,
                    "Timestamp",
                    kotlinx.datetime.Instant.fromEpochMilliseconds(domain.timestamp).toString()
                )
                domain.correlationId?.let { addField(fieldsPanel, "Correlation ID", it) }
                domain.requestId?.let { addField(fieldsPanel, "Request ID", it) }
                addField(fieldsPanel, "Level", domain.level.name)
                addField(fieldsPanel, "Service", "${domain.serviceName}:${domain.serviceVersion}")
                addField(fieldsPanel, "Logger", domain.logger)
                addField(fieldsPanel, "Thread", domain.threadName)
                domain.errorMessage?.let { addField(fieldsPanel, "Error Message", it) }
            }
        }

        return fieldsPanel
    }

    private fun addField(parent: JPanel, label: String, value: String) {
        val fieldPanel = JPanel(BorderLayout()).apply {
            background = UiColors.background
            border = EmptyBorder(2, 0, 2, 0)
            maximumSize = Dimension(Int.MAX_VALUE, 26)
        }

        // Label
        val labelComponent = JLabel("$label:").apply {
            foreground = UiColors.teal
            font = Font(Styles.normalFont, Font.BOLD, 12 - 1)
            preferredSize = Dimension(120, 20)
        }
        fieldPanel.add(labelComponent, BorderLayout.WEST)

        // Value with copy button
        val valuePanel = JPanel(BorderLayout()).apply {
            background = UiColors.background
        }

        val valueField = JTextField(value).apply {
            isEditable = false
            background = UiColors.selectionLine
            foreground = UiColors.defaultText
            font = Font(Styles.monoFont, Font.PLAIN, 12 - 1)
            border = EmptyBorder(3, 8, 3, 8)
            caretColor = UiColors.defaultText
        }
        valuePanel.add(valueField, BorderLayout.CENTER)

        val copyButton = JButton("Copy").apply {
            preferredSize = Dimension(50, 20)
            background = UiColors.backgroundTeal
            foreground = UiColors.defaultText
            border = null
            isFocusPainted = false
            font = Font(Styles.normalFont, Font.PLAIN, 9)
            cursor = Cursor.getPredefinedCursor(Cursor.HAND_CURSOR)

            addActionListener {
                val clipboard = Toolkit.getDefaultToolkit().systemClipboard
                clipboard.setContents(StringSelection(value), null)
            }

            addMouseListener(object : MouseAdapter() {
                override fun mouseEntered(e: MouseEvent) {
                    background = UiColors.backgroundTeal.brighter()
                }

                override fun mouseExited(e: MouseEvent) {
                    background = UiColors.backgroundTeal
                }
            })
        }
        valuePanel.add(copyButton, BorderLayout.EAST)

        fieldPanel.add(valuePanel, BorderLayout.CENTER)
        parent.add(fieldPanel)
    }

    private fun createMessagePanel(): JPanel {
        val messagePanel = JPanel(BorderLayout()).apply {
            background = UiColors.background
            border = EmptyBorder(6, 0, 0, 0)
        }

        // Message label
        val messageLabel = JLabel("Message:").apply {
            foreground = UiColors.teal
            font = Font(Styles.normalFont, Font.BOLD, 12)
            border = EmptyBorder(0, 0, 6, 0)
        }
        messagePanel.add(messageLabel, BorderLayout.NORTH)

        // Message content
        val messageContent = getMessageContent()
        val textPane = JTextPane().apply {
            isEditable = false
            background = UiColors.selectionLine
            foreground = UiColors.defaultText
            font = Font(Styles.monoFont, Font.PLAIN, 12)
            border = EmptyBorder(8, 8, 8, 8)
            caretColor = UiColors.defaultText

            // Set the styled text with syntax highlighting
            setStyledText(this, messageContent)

            // Enable text selection
            addMouseWheelListener { e ->
                parent.dispatchEvent(e)
            }
        }

        val scrollPane = JScrollPane(textPane).apply {
            background = UiColors.background
            border = null
            verticalScrollBarPolicy = JScrollPane.VERTICAL_SCROLLBAR_AS_NEEDED
            horizontalScrollBarPolicy = JScrollPane.HORIZONTAL_SCROLLBAR_AS_NEEDED

            // Style scrollbars
            verticalScrollBar.apply {
                background = UiColors.background
                setUI(ModernScrollBarUI())
            }
            horizontalScrollBar.apply {
                background = UiColors.background
                setUI(ModernScrollBarUI())
            }
        }

        messagePanel.add(scrollPane, BorderLayout.CENTER)

        return messagePanel
    }

    private fun getMessageContent(): String {
        return when (domain) {
            is KafkaLineDomain -> {
                try {
                    json.encodeToString(Json.parseToJsonElement(domain.message))
                } catch (e: Exception) {
                    domain.message
                }
            }

            is LogLineDomain -> {
                buildString {
                    append(domain.message)
                    domain.errorMessage?.let {
                        append("\n\nError: ")
                        append(it)
                    }
                    domain.stacktrace?.let {
                        append("\n\nStack Trace:\n")
                        append(it)
                    }
                }
            }
        }
    }

    private fun setupEventHandlers() {
        // ESC key to close
        val escapeAction = object : AbstractAction() {
            override fun actionPerformed(e: ActionEvent) {
                dispose()
            }
        }

        rootPane.getInputMap(JComponent.WHEN_IN_FOCUSED_WINDOW).put(
            KeyStroke.getKeyStroke(KeyEvent.VK_ESCAPE, 0), "escape"
        )
        rootPane.actionMap.put("escape", escapeAction)

        // Ctrl/Cmd+W to close
        val closeAction = object : AbstractAction() {
            override fun actionPerformed(e: ActionEvent) {
                dispose()
            }
        }

        val closeKeyStroke = if (System.getProperty("os.name").contains("Mac")) {
            KeyStroke.getKeyStroke(KeyEvent.VK_W, InputEvent.META_DOWN_MASK)
        } else {
            KeyStroke.getKeyStroke(KeyEvent.VK_W, InputEvent.CTRL_DOWN_MASK)
        }

        rootPane.getInputMap(JComponent.WHEN_IN_FOCUSED_WINDOW).put(closeKeyStroke, "close")
        rootPane.actionMap.put("close", closeAction)
    }

    // Custom scrollbar UI for modern look
    private class ModernScrollBarUI : javax.swing.plaf.basic.BasicScrollBarUI() {

        override fun configureScrollBarColors() {
            thumbColor = UiColors.defaultText.darker()
            trackColor = UiColors.selectionLine
            thumbHighlightColor = UiColors.defaultText
            thumbLightShadowColor = UiColors.defaultText.darker()
            thumbDarkShadowColor = UiColors.defaultText.darker().darker()
        }

        override fun createDecreaseButton(orientation: Int): JButton {
            return createZeroSizeButton()
        }

        override fun createIncreaseButton(orientation: Int): JButton {
            return createZeroSizeButton()
        }

        private fun createZeroSizeButton(): JButton {
            return JButton().apply {
                preferredSize = Dimension(0, 0)
                minimumSize = Dimension(0, 0)
                maximumSize = Dimension(0, 0)
            }
        }

        override fun paintThumb(g: Graphics, c: JComponent, thumbBounds: java.awt.Rectangle) {
            if (thumbBounds.isEmpty || !scrollbar.isEnabled) {
                return
            }

            val g2 = g.create() as Graphics2D
            g2.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON)

            // Draw rounded thumb
            g2.color = if (isDragging) thumbHighlightColor else thumbColor
            g2.fillRoundRect(
                thumbBounds.x + 2,
                thumbBounds.y + 2,
                thumbBounds.width - 4,
                thumbBounds.height - 4,
                6, 6
            )

            g2.dispose()
        }

        override fun paintTrack(g: Graphics, c: JComponent, trackBounds: java.awt.Rectangle) {
            val g2 = g.create() as Graphics2D
            g2.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON)

            // Draw rounded track
            g2.color = trackColor
            g2.fillRoundRect(
                trackBounds.x,
                trackBounds.y,
                trackBounds.width,
                trackBounds.height,
                8, 8
            )

            g2.dispose()
        }

        override fun getPreferredSize(c: JComponent): Dimension {
            return if (scrollbar.orientation == JScrollBar.VERTICAL) {
                Dimension(12, 0)
            } else {
                Dimension(0, 12)
            }
        }
    }
}