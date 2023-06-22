package slides

import slides.SlideColors.background
import java.awt.Color
import java.awt.image.BufferedImage

class Slide5 : Slide() {
    override fun slide(width: Int, height: Int): BufferedImage {
        val image = BufferedImage(width, height, BufferedImage.TYPE_INT_RGB)
        val g2d = image.createGraphics()

        g2d.color = background
        g2d.fillRect(0, 0, width, height)

        Styles.title(g2d, width, "1 million ganger raskere", height / 8)
         (0 until 500).forEach { x ->
            (0 until 200).forEach { y ->
                g2d.fillRect(x * 3 + (width / 20), y * 3 + (height / 5), 1, 1)
            }
        }
        g2d.color = Color.green
        g2d.fillRect( (width / 20),  (height / 5), 1, 1)


        Styles.title(g2d, width, "* 10", (height- ( (height / 9)) ))
        g2d.dispose()
        return image
    }
}