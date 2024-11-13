package widgets

import app.LogJson
import util.UiColors
import util.Styles
import java.awt.BorderLayout
import java.awt.Font
import java.awt.GridBagConstraints
import java.awt.GridBagLayout
import java.awt.Insets
import java.awt.MouseInfo
import java.awt.Point
import java.awt.Toolkit
import java.awt.datatransfer.StringSelection
import java.awt.event.WindowAdapter
import java.awt.event.WindowEvent
import javax.swing.JButton
import javax.swing.JFrame
import javax.swing.JLabel
import javax.swing.JPanel
import javax.swing.JScrollPane
import javax.swing.JTextArea
import javax.swing.JTextField
import javax.swing.SwingConstants
import javax.swing.ImageIcon

class TextViewer(title: String = "", text: String, logJson: LogJson?) : JFrame() {

    init {
        super.setTitle(title)
        setSize(800, 600)

        val mouseLocation: Point = MouseInfo.getPointerInfo().location
        setLocation(mouseLocation.x - width / 2, mouseLocation.y - height / 2)

        val fieldsPanel = JPanel(GridBagLayout())
        val constraints = GridBagConstraints().apply {
            gridx = 0
            gridy = GridBagConstraints.RELATIVE
            anchor = GridBagConstraints.WEST
            insets = Insets(2, 2, 2, 2)
        }

        logJson?.let { log ->
            addField("Timestamp", log.timestampString, fieldsPanel, constraints)
            addField("CorrelationId", log.correlationId, fieldsPanel, constraints)
            addField("Message", log.message, fieldsPanel, constraints)
            addField("Error Message", log.errorMessage, fieldsPanel, constraints)
            addField("Level", log.level, fieldsPanel, constraints)
            addField("Application", log.indexIdentifier, fieldsPanel, constraints)
            addField("Stacktrace Type", log.stacktraceType, fieldsPanel, constraints)
            addField("Topic", log.topic, fieldsPanel, constraints)
            addField("Key", log.key, fieldsPanel, constraints)
            addField("Partition", log.partition, fieldsPanel, constraints)
            addField("Offset", log.offset, fieldsPanel, constraints)
            addField("Headers", log.headers, fieldsPanel, constraints)
        }

        // Create the main body text area
        val textArea = JTextArea(text)
        textArea.isEditable = false
        textArea.lineWrap = true
        textArea.font = Font(Styles.normalFont, Font.PLAIN, 12)
        textArea.foreground = UiColors.defaultText
        textArea.background = UiColors.background

        val scrollPane = JScrollPane(textArea)
        scrollPane.horizontalScrollBarPolicy = JScrollPane.HORIZONTAL_SCROLLBAR_NEVER
        scrollPane.verticalScrollBarPolicy = JScrollPane.VERTICAL_SCROLLBAR_AS_NEEDED

        layout = BorderLayout()
        add(fieldsPanel, BorderLayout.NORTH)
        add(scrollPane, BorderLayout.CENTER)

        addWindowListener(object : WindowAdapter() {
            override fun windowClosing(windowEvent: WindowEvent) {
                dispose()
            }
        })
    }

    private fun addField(label: String, value: String, panel: JPanel, constraints: GridBagConstraints) {
        if (value.isNotBlank()) {
            val fieldLabel = JLabel("$label:", SwingConstants.LEFT)
            val copyButton = JButton(ImageIcon("path/to/copy_icon.png")).apply {
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
