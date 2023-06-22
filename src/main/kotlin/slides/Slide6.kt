package slides

import slides.SlideColors.background
import java.awt.Color
import java.awt.image.BufferedImage
import java.io.File
import kotlin.system.measureTimeMillis

class Slide6 : Slide() {
    override fun slide(width: Int, height: Int): BufferedImage {
        val image = BufferedImage(width, height, BufferedImage.TYPE_INT_RGB)
        val g2d = image.createGraphics()

        g2d.color = background
        g2d.fillRect(0, 0, width, height)

        val lines = File("yelp_academic_dataset_review.json").readLines()

        println(measureTimeMillis {
            lines.filter { it.contains("GsMpveyQwg4nWsk1V", true) }
        })

        println(lines.size)


        g2d.color = Color.green
        g2d.fillRect( (width / 20),  (height / 5), 1, 1)


        Styles.title(g2d, width, "* 10", (height- ( (height / 9)) ))
        g2d.dispose()
        return image
    }
}