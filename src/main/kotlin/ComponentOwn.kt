import java.awt.event.KeyListener
import java.awt.event.MouseListener
import java.awt.event.MouseMotionListener
import java.awt.event.MouseWheelListener
import java.awt.image.BufferedImage

abstract class ComponentOwn:  KeyListener, MouseListener, MouseWheelListener, MouseMotionListener {
    var height:Int = 0
    var width:Int = 0
    var x:Int = 0
    var y:Int = 0
    var mouseInside = false
    abstract fun display(width: Int, height: Int,x:Int, y:Int): BufferedImage

    abstract fun repaint(componentOwn: ComponentOwn)
}
