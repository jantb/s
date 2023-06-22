package slides

import java.awt.image.BufferedImage

abstract class Slide {
    val width = 2880
    val height = 2160
    abstract fun slide(width: Int, height: Int): BufferedImage

}