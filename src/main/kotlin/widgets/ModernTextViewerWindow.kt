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

class ModernTextViewerWindow(title: String = "Log Details", private val domain: DomainLine) : JFrame() {
    
    private val json = Json { prettyPrint = true }
    private var fontSize = 12
    
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
            border = EmptyBorder(10, 15, 10, 15)
            preferredSize = Dimension(0, 50)
        }
        
        // Title label
        val titleLabel = JLabel("${if (domain is KafkaLineDomain) "Kafka" else "Log"} Line Details").apply {
            foreground = UiColors.defaultText
            font = Font(Styles.normalFont, Font.BOLD, 14)
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
            border = EmptyBorder(10, 0, 10, 0)
        }
        
        when (domain) {
            is KafkaLineDomain -> {
                addField(fieldsPanel, "Timestamp", kotlinx.datetime.Instant.fromEpochMilliseconds(domain.timestamp).toString())
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
                addField(fieldsPanel, "Timestamp", kotlinx.datetime.Instant.fromEpochMilliseconds(domain.timestamp).toString())
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
            border = EmptyBorder(5, 0, 5, 0)
            maximumSize = Dimension(Int.MAX_VALUE, 35)
        }
        
        // Label
        val labelComponent = JLabel("$label:").apply {
            foreground = UiColors.teal
            font = Font(Styles.normalFont, Font.BOLD, fontSize)
            preferredSize = Dimension(150, 25)
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
            font = Font(Styles.monoFont, Font.PLAIN, fontSize)
            border = EmptyBorder(5, 10, 5, 10)
            caretColor = UiColors.defaultText
        }
        valuePanel.add(valueField, BorderLayout.CENTER)
        
        val copyButton = JButton("Copy").apply {
            preferredSize = Dimension(60, 25)
            background = UiColors.backgroundTeal
            foreground = UiColors.defaultText
            border = null
            isFocusPainted = false
            font = Font(Styles.normalFont, Font.PLAIN, 10)
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
            border = EmptyBorder(10, 0, 0, 0)
        }
        
        // Message label
        val messageLabel = JLabel("Message:").apply {
            foreground = UiColors.teal
            font = Font(Styles.normalFont, Font.BOLD, fontSize)
            border = EmptyBorder(0, 0, 10, 0)
        }
        messagePanel.add(messageLabel, BorderLayout.NORTH)
        
        // Message content
        val messageContent = getMessageContent()
        val textArea = JTextArea(messageContent).apply {
            isEditable = false
            lineWrap = true
            wrapStyleWord = true
            background = UiColors.selectionLine
            foreground = UiColors.defaultText
            font = Font(Styles.monoFont, Font.PLAIN, fontSize)
            border = EmptyBorder(10, 10, 10, 10)
            caretColor = UiColors.defaultText
            
            // Enable text selection
            addMouseWheelListener { e ->
                if (e.isControlDown || e.isMetaDown) {
                    fontSize += -e.wheelRotation
                    fontSize = fontSize.coerceIn(8, 24)
                    font = Font(Styles.monoFont, Font.PLAIN, fontSize)
                } else {
                    parent.dispatchEvent(e)
                }
            }
        }
        
        val scrollPane = JScrollPane(textArea).apply {
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