package slides

import slides.SlideColors.background
import slides.Styles.bulletPoints
import java.awt.Color
import java.awt.image.BufferedImage

class Slide2 : Slide() {
    override fun slide(width: Int, height: Int): BufferedImage {
        val image = BufferedImage(width, height, BufferedImage.TYPE_INT_RGB)
        val g2d = image.createGraphics()

        g2d.color = background
        g2d.fillRect(0, 0, width, height)

        Styles.title(g2d, width, "Introduksjon", height / 8)
        bulletPoints(g2d, width, height, listOf("Utvikler i Alv", "Kodet i mange år"))

        g2d.dispose()
        return image
    }
}