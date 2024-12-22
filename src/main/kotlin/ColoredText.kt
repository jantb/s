import util.UiColors
import java.awt.Color
import java.awt.Graphics2D
import javax.swing.text.html.HTML.Tag.P

class ColoredText() {
    private var textList = mutableListOf<String>()
    var text = ""
    private val colorList = mutableListOf<Color>()
    var highlight = false
    var highlightRange: IntRange? = null
    fun isNotBlank(): Boolean {
        return textList.isNotEmpty()
    }

    fun clear() {
        textList.clear()
        colorList.clear()
        text = ""
    }

    fun addText(text: String, color: Color) {
        colorList += color
        this.textList += text
        this.text += text
    }

    fun print(x: Int, y: Int, graphics: Graphics2D) {
        val color = graphics.color
        val fontMetrics = graphics.fontMetrics

        var xx = x
        textList.forEachIndexed { i, it ->
            graphics.color = colorList[i]
            graphics.drawString(it, xx, y)
            xx += fontMetrics.stringWidth(it)
        }

        if (highlight) {
            val first = text.substring(0, highlightRange!!.first.coerceIn(0..text.length))
            val high = text.substring(
                highlightRange!!.first.coerceIn(0..text.length),
                (highlightRange!!.last + 1).coerceIn(0..text.length)
            )
            graphics.color = UiColors.highlightRect
            graphics.drawString(
                high,
                graphics.fontMetrics.stringWidth(first),
                y
            )
            graphics.color = UiColors.defaultText

        }

        graphics.color = color
    }

    fun getHighlightedText(): String {
        return if (highlight) {
            if (highlightRange == null) {
                ""
            } else {
                text.substring(
                    highlightRange!!.first.coerceIn(0..text.length),
                    (highlightRange!!.last + 1).coerceIn(0..text.length)
                )
            }
        } else {

            ""
        }
    }

}
