import util.UiColors
import java.awt.Color
import java.awt.Graphics2D

class ColoredText {
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

    fun print(x: Int, y: Int, graphics: Graphics2D, maxWidth: Int = Int.MAX_VALUE) {
        val originalColor = graphics.color
        val fontMetrics = graphics.fontMetrics

        var xx = x

        textList.forEachIndexed { i, segment ->
            val color = colorList[i]
            val segmentWidth = fontMetrics.stringWidth(segment)

            if (xx + segmentWidth <= x + maxWidth) {
                // Whole segment fits
                graphics.color = color
                graphics.drawString(segment, xx, y)
                xx += segmentWidth
            } else {
                // Binary search for the largest prefix that fits
                var low = 0
                var high = segment.length
                var bestFit = ""

                while (low <= high) {
                    val mid = (low + high) / 2
                    val sub = segment.substring(0, mid)
                    val w = fontMetrics.stringWidth(sub)
                    if (xx + w <= x + maxWidth) {
                        bestFit = sub
                        low = mid + 1
                    } else {
                        high = mid - 1
                    }
                }

                if (bestFit.isNotEmpty()) {
                    graphics.color = color
                    graphics.drawString(bestFit, xx, y)
                    xx += fontMetrics.stringWidth(bestFit)
                }

                return
            }
        }

        if (highlight && highlightRange != null) {
            val first = text.substring(0, highlightRange!!.first.coerceIn(0..text.length))
            val high = text.substring(
                highlightRange!!.first.coerceIn(0..text.length),
                (highlightRange!!.last + 1).coerceIn(0..text.length)
            )
            val startX = x + fontMetrics.stringWidth(first)
            if (startX < x + maxWidth) {
                graphics.color = UiColors.highlightRect
                graphics.drawString(high, startX, y)
            }
            graphics.color = UiColors.defaultText
        }

        graphics.color = originalColor
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
