package slides

import slides.SlideColors.background
import slides.Styles.bulletPoints
import slides.Styles.centerTitle
import java.awt.Color
import java.awt.image.BufferedImage

class Slide4 : Slide() {
    override fun slide(width: Int, height: Int): BufferedImage {
        val image = BufferedImage(width, height, BufferedImage.TYPE_INT_RGB)
        val g2d = image.createGraphics()

        g2d.color = background
        g2d.fillRect(0, 0, width, height)

        centerTitle(g2d, width, height, "For tregt")

        g2d.dispose()
        return image
    }
}