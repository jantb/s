package widgets

import app.LogLineDomain
import util.Styles
import util.UiColors
import java.awt.*
import java.awt.datatransfer.StringSelection
import java.awt.event.MouseWheelEvent
import java.awt.event.WindowAdapter
import java.awt.event.WindowEvent
import javax.swing.*

class TextViewerLogLine(title: String = "", domain: LogLineDomain?) : JFrame() {

    private var textAreaFontSize = 12

    init {
        super.setTitle(title)
        setSize(800, 600)

        val mouseLocation: Point = MouseInfo.getPointerInfo().location
        setLocation(mouseLocation.x - width / 2, mouseLocation.y - height / 2)

        val topPanel = JPanel(BorderLayout())
        val fieldsPanel = JPanel(GridBagLayout())
        val constraints = GridBagConstraints().apply {
            gridx = 0
            gridy = GridBagConstraints.RELATIVE
            anchor = GridBagConstraints.WEST
            insets = Insets(0, 2, 0, 2)
        }

        domain?.let { log ->
            addField("Timestamp", kotlinx.datetime.Instant.fromEpochMilliseconds(log.timestamp).toString(), fieldsPanel, constraints)
            log.correlationId?.let {
                addField("CorrelationId", it, fieldsPanel, constraints)
            }
            log.requestId?.let {
                addField("RequestId", it, fieldsPanel, constraints)
            }
            log.errorMessage?.let {
                addField("Error Message", it, fieldsPanel, constraints)
            }

            addField("Level", log.level.name, fieldsPanel, constraints)
            addField("Application", "${log.serviceName}:${log.serviceVersion}", fieldsPanel, constraints)

            val textArea =
                JTextArea(log.message + " " + (log.errorMessage ?: "") + " " + (log.stacktrace ?: "")).apply {
                    addMouseWheelListener { e ->
                        if (e.isControlDown || e.isMetaDown) {
                            textAreaFontSize += -e.wheelRotation
                            if (textAreaFontSize < 8) textAreaFontSize = 8
                            font = Font(Styles.normalFont, Font.PLAIN, textAreaFontSize)
                        } else {
                            parent.dispatchEvent(
                                MouseWheelEvent(
                                    parent,
                                    e.id,
                                    e.getWhen(),
                                    e.getModifiersEx(),
                                    e.x,
                                    e.y,
                                    e.clickCount,
                                    e.isPopupTrigger,
                                    e.scrollType,
                                    e.scrollAmount,
                                    e.wheelRotation
                                )
                            )
                        }
                    }

                    isEditable = false
                    lineWrap = true
                    font = Font(Styles.normalFont, Font.PLAIN, textAreaFontSize)
                    foreground = UiColors.defaultText
                    background = UiColors.background
                }

            val scrollPane = JScrollPane(textArea)
            scrollPane.horizontalScrollBarPolicy = JScrollPane.HORIZONTAL_SCROLLBAR_NEVER
            scrollPane.verticalScrollBarPolicy = JScrollPane.VERTICAL_SCROLLBAR_AS_NEEDED

            layout = BorderLayout()

            topPanel.add(fieldsPanel, BorderLayout.CENTER)
            topPanel.add(createAlwaysOnTopToggleButton(), BorderLayout.EAST)
            add(topPanel, BorderLayout.NORTH)
            add(scrollPane, BorderLayout.CENTER)
        }
        addWindowListener(object : WindowAdapter() {
            override fun windowClosing(windowEvent: WindowEvent) {
                dispose()
            }
        })
    }

    private fun createAlwaysOnTopToggleButton(): JToggleButton {
        val toggleButton = JToggleButton(ImageIcon(ClassLoader.getSystemResource("ontop.png")))
        toggleButton.addActionListener {
            isAlwaysOnTop = toggleButton.isSelected
        }
        return toggleButton
    }

    private fun addField(label: String, value: String, panel: JPanel, constraints: GridBagConstraints) {
        if (value.isNotBlank()) {
            val fieldLabel = JLabel("$label:", SwingConstants.LEFT)
            val copyButton = JButton(ImageIcon(ClassLoader.getSystemResource("copy.png"))).apply {
                toolTipText = "Copy to clipboard"
                addActionListener {
                    val stringSelection = StringSelection(value)
                    val clipboard = Toolkit.getDefaultToolkit().systemClipboard
                    clipboard.setContents(stringSelection, null)
                }
            }

            val fieldValue = JTextField(value).apply {
                isEditable = false
                minimumSize = preferredSize
                columns = 50
            }

            constraints.gridx = 0
            constraints.weightx = 0.0
            constraints.fill = GridBagConstraints.NONE
            panel.add(fieldLabel, constraints)

            constraints.gridx = 1
            constraints.weightx = 0.0
            constraints.fill = GridBagConstraints.NONE
            panel.add(copyButton, constraints)

            constraints.gridx = 2
            constraints.weightx = 1.0
            constraints.fill = GridBagConstraints.HORIZONTAL
            panel.add(fieldValue, constraints)
        }
    }
}
