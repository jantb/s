package slides

import slides.SlideColors.background
import java.awt.image.BufferedImage

class Slide3 : Slide() {
    override fun slide(width: Int, height: Int): BufferedImage {
        val image = BufferedImage(width, height, BufferedImage.TYPE_INT_RGB)
        val g2d = image.createGraphics()

        g2d.color = background
        g2d.fillRect(0, 0, width, height)

        Styles.title(g2d, width, "Hvorfor FTS og ikke bruke Elastic?", height / 8)

        g2d.dispose()
        return image
    }
}