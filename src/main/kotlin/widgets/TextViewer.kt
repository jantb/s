package widgets

import com.sun.java.accessibility.util.AWTEventMonitor.addWindowListener
import slides.SlideColors
import slides.Styles
import java.awt.Font
import java.awt.Frame
import java.awt.event.WindowAdapter
import java.awt.event.WindowEvent
import javax.swing.JFrame
import javax.swing.JScrollPane
import javax.swing.JTextArea

class TextViewer(title: String = "", text: String) : JFrame() {

    init {
        super.setTitle(title)
        setSize(800, 600)

        val textArea = JTextArea(text)
        textArea.isEditable = false
        textArea.lineWrap = true
        textArea.font = Font(Styles.normalFont, Font.PLAIN, 12)
        textArea.foreground = SlideColors.defaultText
        textArea.background = SlideColors.background

        val scrollPane = JScrollPane(textArea)
        scrollPane.horizontalScrollBarPolicy = JScrollPane.HORIZONTAL_SCROLLBAR_NEVER
        scrollPane.verticalScrollBarPolicy = JScrollPane.VERTICAL_SCROLLBAR_AS_NEEDED

        contentPane.add(scrollPane)

        addWindowListener(object : WindowAdapter() {
            override fun windowClosing(windowEvent: WindowEvent) {
                dispose()
            }
        })
    }
}
