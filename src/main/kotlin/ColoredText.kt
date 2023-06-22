import slides.SlideColors
import java.awt.Color
import java.awt.Graphics2D

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
            graphics.color = SlideColors.highlightRect
            graphics.drawString(
                high,
                graphics.fontMetrics.stringWidth(first),
                y
            )
            graphics.color = SlideColors.defaultText

        }

        graphics.color = color
    }

    fun getHighlightedText(): String {
        return if (highlight) {
            text.substring(
                highlightRange!!.first.coerceIn(0..text.length),
                (highlightRange!!.last + 1).coerceIn(0..text.length)
            )
        } else {

            ""
        }
    }

}
