package widgets

import slides.SlideColors
import slides.Styles
import java.awt.Font
import java.awt.Frame
import java.awt.event.WindowAdapter
import java.awt.event.WindowEvent
import javax.swing.JTextArea

class TextViewer(title: String = "", text: String) : Frame() {

    init {
        super.setTitle(title)
        setSize(800, 600)

        val textArea = JTextArea()
        textArea.isEditable = false
        textArea.lineWrap = true
        textArea.font = Font(Styles.normalFont, Font.PLAIN, 12)
        textArea.text = text
        textArea.foreground = SlideColors.defaultText
        textArea.background = SlideColors.background

        add(textArea)
        addWindowListener(object : WindowAdapter() {
            override fun windowClosing(windowEvent: WindowEvent) {
                dispose()
            }
        })
    }
}
