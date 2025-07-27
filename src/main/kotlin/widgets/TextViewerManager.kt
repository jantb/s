package widgets

import ComponentOwn
import SlidePanel
import app.DomainLine
import java.awt.event.KeyEvent
import java.awt.event.MouseEvent
import java.awt.event.MouseWheelEvent
import java.awt.image.BufferedImage

class TextViewerManager(private val parent: SlidePanel) : ComponentOwn() {
    private var currentViewer: ModernTextViewer? = null
    
    fun showViewer(domain: DomainLine, width: Int, height: Int) {
        hideViewer()
        
        // Center the viewer on screen
        val viewerWidth = (width * 0.8).toInt().coerceAtLeast(600)
        val viewerHeight = (height * 0.8).toInt().coerceAtLeast(400)
        val viewerX = (width - viewerWidth) / 2
        val viewerY = (height - viewerHeight) / 2
        
        currentViewer = ModernTextViewer(
            parent = this,
            domain = domain,
            x = viewerX,
            y = viewerY,
            width = viewerWidth,
            height = viewerHeight
        )
        currentViewer?.show()
    }
    
    fun hideViewer() {
        currentViewer?.hide()
        currentViewer = null
        parent.repaint()
    }
    
    fun isViewerVisible(): Boolean = currentViewer != null
    
    override fun display(width: Int, height: Int, x: Int, y: Int): BufferedImage {
        // Create a transparent overlay
        val image = BufferedImage(width, height, BufferedImage.TYPE_INT_ARGB)
        val g2d = image.createGraphics()
        
        currentViewer?.let { viewer ->
            // Draw semi-transparent background overlay
            g2d.color = java.awt.Color(0, 0, 0, 128)
            g2d.fillRect(0, 0, width, height)
            
            // Draw the viewer
            val viewerImage = viewer.display(viewer.width, viewer.height, viewer.x, viewer.y)
            g2d.drawImage(viewerImage, viewer.x, viewer.y, null)
        }
        
        g2d.dispose()
        return image
    }
    
    override fun repaint(componentOwn: ComponentOwn) {
        parent.repaint()
    }
    
    override fun keyTyped(e: KeyEvent?) {
        currentViewer?.keyTyped(e)
    }
    
    override fun keyPressed(e: KeyEvent?) {
        currentViewer?.keyPressed(e)
        if (e?.keyCode == KeyEvent.VK_ESCAPE) {
            hideViewer()
        }
    }
    
    override fun keyReleased(e: KeyEvent?) {
        currentViewer?.keyReleased(e)
    }
    
    override fun mouseClicked(e: MouseEvent) {
        currentViewer?.let { viewer ->
            if (isMouseInViewer(e, viewer)) {
                // Adjust coordinates relative to viewer
                val adjustedEvent = MouseEvent(
                    e.component, e.id, e.`when`, e.modifiersEx,
                    e.x - viewer.x, e.y - viewer.y,
                    e.clickCount, e.isPopupTrigger, e.button
                )
                viewer.mouseClicked(adjustedEvent)
            } else {
                // Click outside viewer - hide it
                hideViewer()
            }
        }
    }
    
    override fun mousePressed(e: MouseEvent?) {
        e?.let { event ->
            currentViewer?.let { viewer ->
                if (isMouseInViewer(event, viewer)) {
                    val adjustedEvent = MouseEvent(
                        event.component, event.id, event.`when`, event.modifiersEx,
                        event.x - viewer.x, event.y - viewer.y,
                        event.clickCount, event.isPopupTrigger, event.button
                    )
                    viewer.mousePressed(adjustedEvent)
                }
            }
        }
    }
    
    override fun mouseReleased(e: MouseEvent?) {
        e?.let { event ->
            currentViewer?.let { viewer ->
                if (isMouseInViewer(event, viewer)) {
                    val adjustedEvent = MouseEvent(
                        event.component, event.id, event.`when`, event.modifiersEx,
                        event.x - viewer.x, event.y - viewer.y,
                        event.clickCount, event.isPopupTrigger, event.button
                    )
                    viewer.mouseReleased(adjustedEvent)
                }
            }
        }
    }
    
    override fun mouseEntered(e: MouseEvent) {
        currentViewer?.let { viewer ->
            if (isMouseInViewer(e, viewer)) {
                val adjustedEvent = MouseEvent(
                    e.component, e.id, e.`when`, e.modifiersEx,
                    e.x - viewer.x, e.y - viewer.y,
                    e.clickCount, e.isPopupTrigger, e.button
                )
                viewer.mouseEntered(adjustedEvent)
            }
        }
    }
    
    override fun mouseExited(e: MouseEvent) {
        currentViewer?.let { viewer ->
            val adjustedEvent = MouseEvent(
                e.component, e.id, e.`when`, e.modifiersEx,
                e.x - viewer.x, e.y - viewer.y,
                e.clickCount, e.isPopupTrigger, e.button
            )
            viewer.mouseExited(adjustedEvent)
        }
    }
    
    override fun mouseWheelMoved(e: MouseWheelEvent?) {
        e?.let { event ->
            currentViewer?.let { viewer ->
                if (isMouseInViewer(event, viewer)) {
                    val adjustedEvent = MouseWheelEvent(
                        event.component, event.id, event.`when`, event.modifiersEx,
                        event.x - viewer.x, event.y - viewer.y,
                        event.clickCount, event.isPopupTrigger,
                        event.scrollType, event.scrollAmount, event.wheelRotation
                    )
                    viewer.mouseWheelMoved(adjustedEvent)
                }
            }
        }
    }
    
    override fun mouseDragged(e: MouseEvent?) {
        e?.let { event ->
            currentViewer?.let { viewer ->
                if (isMouseInViewer(event, viewer)) {
                    val adjustedEvent = MouseEvent(
                        event.component, event.id, event.`when`, event.modifiersEx,
                        event.x - viewer.x, event.y - viewer.y,
                        event.clickCount, event.isPopupTrigger, event.button
                    )
                    viewer.mouseDragged(adjustedEvent)
                }
            }
        }
    }
    
    override fun mouseMoved(e: MouseEvent) {
        currentViewer?.let { viewer ->
            if (isMouseInViewer(e, viewer)) {
                val adjustedEvent = MouseEvent(
                    e.component, e.id, e.`when`, e.modifiersEx,
                    e.x - viewer.x, e.y - viewer.y,
                    e.clickCount, e.isPopupTrigger, e.button
                )
                viewer.mouseMoved(adjustedEvent)
            }
        }
    }
    
    private fun isMouseInViewer(e: MouseEvent, viewer: ModernTextViewer): Boolean {
        return e.x >= viewer.x && e.x <= viewer.x + viewer.width &&
               e.y >= viewer.y && e.y <= viewer.y + viewer.height
    }
}