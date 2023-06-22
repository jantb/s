package slides

import java.awt.Color
import java.awt.Font
import java.awt.Graphics2D
import java.awt.LinearGradientPaint

object Styles {
    val monoFont = "JetBrainsMono Nerd Font Mono"
    val normalFont = "JetBrainsMono Nerd Font"
    fun title(g2d: Graphics2D, width: Int, title: String, height: Int) {

        g2d.font = Font(normalFont, Font.BOLD, width / 25)
        val rose = Color(226, 68, 98)
        val purple = Color(177, 37, 234)
        val purpleBlue = Color(127, 82, 255)

        val colors = arrayOf(rose, purple, purpleBlue)

        val textWidth = g2d.fontMetrics.stringWidth(title)
        g2d.paint = LinearGradientPaint(0f, 0f, textWidth.toFloat(), 0f, floatArrayOf(0f, 0.5f, 1f), colors)
        val x = (width - textWidth) / 2
        val y = height
        g2d.drawString(title, x, y)
    }

    fun centerTitle(g2d: Graphics2D, width: Int, height: Int, title: String) {

        val rose = Color(226, 68, 98)
        val purple = Color(177, 37, 234)
        val purpleBlue = Color(127, 82, 255)

        val colors = arrayOf(rose, purple, purpleBlue)

        g2d.font = Font(normalFont, Font.BOLD, width / 20)

        val fontMetrics = g2d.fontMetrics
        val textWidth = fontMetrics.stringWidth(title)
        g2d.paint = LinearGradientPaint(0f, 0f, 500f, 0f, floatArrayOf(0f, 0.5f, 1f), colors)
        val textHeight = fontMetrics.height
        val x = (width - textWidth) / 2
        val y = (height + textHeight) / 2
        g2d.drawString(title, x, y)
    }

    fun bulletPoints(g2d: Graphics2D, width: Int, height: Int, strings: List<String>) {
        g2d.font = Font(normalFont, Font.BOLD, width / 40)
        val rose = Color(226, 68, 98)
        val purple = Color(177, 37, 234)
        val purpleBlue = Color(127, 82, 255)

        val colors = arrayOf(rose, purple, purpleBlue)

        val bulletPoints = strings
        val fontMetrics = g2d.fontMetrics
        g2d.paint = LinearGradientPaint(0f, 0f, 500.toFloat(), 0f, floatArrayOf(0f, 0.5f, 1f), colors)

        bulletPoints.forEachIndexed { i, it ->
            val x = (width) / 8
            var y = height / 4

            y += i * (fontMetrics.height * 1.5).toInt()
            val bulletSize = width / 120
            g2d.fillOval(
                x - (bulletSize * 3),
                y + (bulletSize / 2),
                bulletSize,
                bulletSize
            )
            g2d.drawString(it, x, y + fontMetrics.height / 2)
        }
    }
}