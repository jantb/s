package widgets

import ComponentOwn
import Mode
import SlidePanel
import State
import util.Styles
import util.UiColors
import java.awt.Font
import java.awt.Graphics2D
import java.awt.event.*
import java.awt.geom.Rectangle2D
import java.awt.image.BufferedImage
import kotlin.math.abs

/**
 * Base class for modal selectors with common functionality
 */
abstract class BaseSelector<T : SelectableItem>(
    protected val panel: SlidePanel,
    x: Int, y: Int, width: Int, height: Int
) : ComponentOwn(), KeyListener, MouseListener, MouseWheelListener, MouseMotionListener {
    
    protected val items: MutableList<T> = mutableListOf()
    protected val filteredItems: MutableList<T> = mutableListOf()
    protected var filterText = ""
    protected var selectedLineIndex = 0
    protected var rowHeight = 14
    protected var isModalOpen = false
    
    private lateinit var image: BufferedImage
    private lateinit var g2d: Graphics2D
    private lateinit var maxCharBounds: Rectangle2D
    
    // Modal dimensions - will be calculated based on window size
    private var modalWidth = 600
    private var modalHeight = 400
    private var modalX = 0
    private var modalY = 0
    
    init {
        this.x = x
        this.y = y
        this.height = height
        this.width = width
    }
    
    override fun display(width: Int, height: Int, x: Int, y: Int): BufferedImage {
        if (this.width != width || this.height != height || this.x != x || this.y != y) {
            if (::g2d.isInitialized) {
                this.g2d.dispose()
            }
            this.image = BufferedImage(width, height, BufferedImage.TYPE_INT_RGB)
            this.g2d = this.image.createGraphics()
            this.height = height
            this.width = width
            this.x = x
            this.y = y
            
            // Calculate modal dimensions: window size - 100px on each side
            modalWidth = (width - 200).coerceAtLeast(400)
            modalHeight = (height - 200).coerceAtLeast(300)
            
            // Center modal
            modalX = (width - modalWidth) / 2
            modalY = (height - modalHeight) / 2
        }
        
        // Modal opening is now handled by keyPressed() method
        // No auto-opening here to avoid conflicts
        
        // Clear background
        g2d.color = UiColors.background
        g2d.fillRect(0, 0, width, height)
        
        if (isModalOpen) {
            drawModal()
        } else {
            drawClosedState()
        }
        
        return image
    }
    
    private fun drawModal() {
        // Draw semi-transparent overlay
        g2d.color = java.awt.Color(0, 0, 0, 128)
        g2d.fillRect(0, 0, width, height)
        
        // Draw modal background
        g2d.color = UiColors.background
        g2d.fillRect(modalX, modalY, modalWidth, modalHeight)
        
        // Draw modal border
        g2d.color = UiColors.selectionLine
        g2d.drawRect(modalX, modalY, modalWidth, modalHeight)
        
        // Set font and get metrics
        g2d.font = Font(Styles.normalFont, Font.PLAIN, rowHeight)
        maxCharBounds = g2d.fontMetrics.getMaxCharBounds(g2d)
        
        // Draw title
        g2d.color = UiColors.magenta
        val title = getTitle()
        val titleBounds = g2d.fontMetrics.getStringBounds(title, g2d)
        g2d.drawString(title, modalX + 10, modalY + 25)
        
        // Draw filter text
        g2d.color = UiColors.defaultText
        val filterDisplay = "Filter: $filterText"
        g2d.drawString(filterDisplay, modalX + 10, modalY + 50)
        
        // Draw selection count
        val selectedCount = items.count { it.selected }
        val countText = "Selected: $selectedCount/${items.size}"
        val countBounds = g2d.fontMetrics.getStringBounds(countText, g2d)
        g2d.drawString(countText, modalX + modalWidth - countBounds.width.toInt() - 10, modalY + 25)
        
        // Draw items list
        drawItemsList()
        
        // Draw instructions
        drawInstructions()
    }
    
    private fun drawItemsList() {
        val listStartY = modalY + 70
        val listHeight = modalHeight - 160  // Reserve more space for instructions at bottom
        val lineHeight = maxCharBounds.height.toInt() + 4
        val maxVisibleItems = listHeight / lineHeight
        
        // Calculate scroll offset to keep selected item visible
        val scrollOffset = if (selectedLineIndex >= maxVisibleItems) {
            selectedLineIndex - maxVisibleItems + 1
        } else 0
        
        // Draw selection highlight
        if (filteredItems.isNotEmpty() && selectedLineIndex < filteredItems.size && selectedLineIndex >= scrollOffset) {
            g2d.color = UiColors.selectionLine
            val highlightY = listStartY + (selectedLineIndex - scrollOffset) * lineHeight
            g2d.fillRect(modalX + 5, highlightY - maxCharBounds.height.toInt(), modalWidth - 10, lineHeight)
        }
        
        // Draw items
        filteredItems.drop(scrollOffset).take(maxVisibleItems).forEachIndexed { index, item ->
            val actualIndex = index + scrollOffset
            val itemY = listStartY + index * lineHeight
            
            // Set color based on selection state
            g2d.color = when {
                item.selected -> UiColors.green
                actualIndex == selectedLineIndex -> UiColors.magenta
                else -> UiColors.defaultText
            }
            
            // Draw checkbox
            val checkbox = if (item.selected) "☑" else "☐"
            g2d.drawString(checkbox, modalX + 10, itemY)
            
            // Draw item text with clipping to prevent overflow
            val itemText = getItemDisplayText(item)
            val availableWidth = modalWidth - 50 // Leave space for checkbox and margins
            val clippedText = clipTextToWidth(itemText, availableWidth)
            g2d.drawString(clippedText, modalX + 35, itemY)
        }
    }
    
    private fun clipTextToWidth(text: String, maxWidth: Int): String {
        val textWidth = g2d.fontMetrics.stringWidth(text)
        if (textWidth <= maxWidth) {
            return text
        }
        
        // Binary search to find the longest substring that fits
        var left = 0
        var right = text.length
        var result = ""
        
        while (left <= right) {
            val mid = (left + right) / 2
            val substring = text.substring(0, mid) + "..."
            val width = g2d.fontMetrics.stringWidth(substring)
            
            if (width <= maxWidth) {
                result = substring
                left = mid + 1
            } else {
                right = mid - 1
            }
        }
        
        return if (result.isEmpty()) "..." else result
    }
    
    private fun drawInstructions() {
        val instructionsY = modalY + modalHeight - 40
        g2d.color = UiColors.magenta
        g2d.font = Font(Styles.normalFont, Font.PLAIN, 10)
        
        val instructions = "↑↓: Navigate | Enter: Toggle | Cmd+A: Toggle All | Cmd+D: Clear All | Esc: Close | Type to filter"
        g2d.drawString(instructions, modalX + 10, instructionsY)
    }
    
    private fun drawClosedState() {
        g2d.font = Font(Styles.normalFont, Font.PLAIN, rowHeight)
        g2d.color = UiColors.defaultText
        
        val selectedCount = items.count { it.selected }
        val statusText = "${getTitle()}: $selectedCount selected (${getShortcutKey()} to open)"
        g2d.drawString(statusText, 10, 30)
        
        // Show selected items preview
        val selectedItems = items.filter { it.selected }.take(5)
        selectedItems.forEachIndexed { index, item ->
            g2d.color = UiColors.green
            g2d.drawString("• ${getItemDisplayText(item)}", 20, 60 + index * 20)
        }
        
        if (selectedCount > 5) {
            g2d.color = UiColors.defaultText
            g2d.drawString("... and ${selectedCount - 5} more", 20, 60 + 5 * 20)
        }
    }
    
    override fun keyPressed(e: KeyEvent) {
        // If modal is not open and this is the shortcut key, open it
        if (!isModalOpen && shouldOpenModal(e)) {
            openModal()
            panel.repaint()
            return
        }
        
        // If modal is not open, ignore other keys
        if (!isModalOpen) {
            return
        }
        
        when {
            // Handle escape key
            e.keyCode == KeyEvent.VK_ESCAPE -> {
                closeModal()
                panel.repaint()
            }
            
            // Handle shortcut to close modal (same key that opens it)
            shouldOpenModal(e) -> {
                closeModal()
                panel.repaint()
            }
            
            // Handle Ctrl/Cmd+A for select all
            ((e.isMetaDown && State.onMac) || (e.isControlDown && !State.onMac)) && e.keyCode == KeyEvent.VK_A -> {
                toggleAllItems()
                panel.repaint()
            }
            
            // Handle Ctrl/Cmd+D for deselect all (clear all)
            ((e.isMetaDown && State.onMac) || (e.isControlDown && !State.onMac)) && e.keyCode == KeyEvent.VK_D -> {
                clearAllItems()
                panel.repaint()
            }
            
            // Handle navigation keys
            e.keyCode == KeyEvent.VK_DOWN -> {
                selectedLineIndex = (selectedLineIndex + 1).coerceIn(0 until filteredItems.size)
                panel.repaint()
            }
            
            e.keyCode == KeyEvent.VK_UP -> {
                selectedLineIndex = (selectedLineIndex - 1).coerceIn(0 until filteredItems.size)
                panel.repaint()
            }
            
            // Handle enter key for selection
            e.keyCode == KeyEvent.VK_ENTER -> {
                if (filteredItems.isNotEmpty() && selectedLineIndex < filteredItems.size) {
                    val item = filteredItems[selectedLineIndex]
                    toggleItemSelection(item)
                    panel.repaint()
                }
            }
            
            // Handle backspace for filtering
            e.keyCode == KeyEvent.VK_BACK_SPACE -> {
                if (filterText.isNotEmpty()) {
                    filterText = filterText.dropLast(1)
                    applyFilter()
                    panel.repaint()
                }
            }
            
            // Handle character input for filtering (only if no modifiers are pressed)
            !e.isMetaDown && !e.isControlDown && !e.isAltDown &&
            (e.keyChar.isLetterOrDigit() || e.keyChar == ' ' || e.keyChar == '-' || e.keyChar == '_') -> {
                filterText += e.keyChar
                applyFilter()
                panel.repaint()
            }
        }
    }
    
    private fun openModal() {
        isModalOpen = true
        refreshItems()
        applyFilter()
    }
    
    private fun closeModal() {
        isModalOpen = false
        filterText = ""
        selectedLineIndex = 0
        onModalClosed()
        
        // Switch back to viewer mode
        State.mode = Mode.viewer
        panel.repaint()
    }
    
    private fun toggleAllItems() {
        val allSelected = filteredItems.all { it.selected }
        filteredItems.forEach { item ->
            item.selected = !allSelected
            if (item.selected) {
                onItemSelected(item)
            } else {
                onItemDeselected(item)
            }
        }
    }
    
    private fun clearAllItems() {
        items.forEach { item ->
            if (item.selected) {
                item.selected = false
                onItemDeselected(item)
            }
        }
        // Update filtered items to reflect the changes
        applyFilter()
    }
    
    private fun toggleItemSelection(item: T) {
        item.selected = !item.selected
        if (item.selected) {
            onItemSelected(item)
        } else {
            onItemDeselected(item)
        }
    }
    
    private fun applyFilter() {
        filteredItems.clear()
        if (filterText.isEmpty()) {
            filteredItems.addAll(items)
        } else {
            filteredItems.addAll(items.filter {
                getItemDisplayText(it).contains(filterText, ignoreCase = true)
            })
        }
        selectedLineIndex = if (filteredItems.isEmpty()) 0 else selectedLineIndex.coerceIn(0 until filteredItems.size)
    }
    
    override fun mouseClicked(e: MouseEvent) {
        if (!isModalOpen) return
        
        // Check if click is outside modal to close it
        if (e.x < modalX || e.x > modalX + modalWidth || e.y < modalY || e.y > modalY + modalHeight) {
            closeModal()
            panel.repaint()
            return
        }
        
        // Handle clicks on items
        val listStartY = modalY + 70
        val clickedIndex = (e.y - listStartY) / (maxCharBounds.height.toInt() + 2)
        if (clickedIndex >= 0 && clickedIndex < filteredItems.size) {
            selectedLineIndex = clickedIndex
            val item = filteredItems[clickedIndex]
            toggleItemSelection(item)
            panel.repaint()
        }
    }
    
    // Abstract methods to be implemented by subclasses
    abstract fun getTitle(): String
    abstract fun getShortcutKey(): String
    abstract fun shouldOpenModal(e: KeyEvent): Boolean
    abstract fun getItemDisplayText(item: T): String
    abstract fun refreshItems()
    abstract fun onItemSelected(item: T)
    abstract fun onItemDeselected(item: T)
    abstract fun onModalClosed()
    
    // Default implementations for unused mouse events
    override fun keyTyped(e: KeyEvent) {}
    override fun keyReleased(e: KeyEvent) {}
    override fun mousePressed(e: MouseEvent) {}
    override fun mouseReleased(e: MouseEvent) {}
    override fun mouseEntered(e: MouseEvent) {}
    override fun mouseExited(e: MouseEvent) {}
    override fun mouseWheelMoved(e: MouseWheelEvent) {}
    override fun mouseDragged(e: MouseEvent) {}
    override fun mouseMoved(e: MouseEvent) {}
    override fun repaint(componentOwn: ComponentOwn) {}
}

/**
 * Interface for selectable items
 */
interface SelectableItem {
    var selected: Boolean
}