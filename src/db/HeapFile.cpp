#include <db/Database.hpp>
#include <db/HeapFile.hpp>
#include <db/HeapPage.hpp>
#include <stdexcept>

using namespace db;

HeapFile::HeapFile(const std::string &name, const TupleDesc &td) : DbFile(name, td) {}

void HeapFile::insertTuple(const Tuple &t) {
  if (!td.compatible(t)) {
    throw std::invalid_argument("Tuple is not compatible with the schema");
  }

  // A container to track pages that are modified (like the "pages" list in the example)
  std::vector<Page> pages;

  // Retrieve the current number of pages
  size_t currentNumPages = getNumPages();

  // Traverse through all the pages in the file
  for (size_t i = 0; i < currentNumPages; ++i) {
    Page page;

    // Fetch the page from the BufferPool (or read from the file)
    page = getDatabase().getBufferPool().getPage(PageId{name, i});

    // Create a HeapPage object to operate on the page
    HeapPage hp(page, getTupleDesc());

    // Iterate over the slots from the beginning to the end
    for (size_t slot = hp.begin(); slot != hp.end(); ++slot) {
      if (hp.empty(slot)) {
        // If there's an empty slot, insert the tuple
        if(hp.insertTuple(t)) {
          // Mark the page as dirty because we modified it
          getDatabase().getBufferPool().markDirty(PageId{name, i});

          // Add the modified page to the list
          pages.push_back(page);
          return; // Stop after inserting the tuple
        }
      }
    }
  }

  // If all pages are full, create a new page and insert the tuple
  Page newPage;
  HeapPage newHp(newPage, getTupleDesc());

  // Insert the tuple into the new page
  if (!newHp.insertTuple(t)) {
    throw std::runtime_error("Failed to insert tuple into a new page");
  }

  // Write the new page to the file and mark it as dirty
  writePage(newPage, currentNumPages);  // Insert new page at the end of the file
  getDatabase().getBufferPool().markDirty(PageId{name, currentNumPages});

  // Increment the page count after adding the new page
  ++numPages;

  // Add the new page to the list of modified pages
  pages.push_back(newPage);
}



void HeapFile::deleteTuple(const Iterator &it) {
  if (it.page >= numPages) {
    throw std::out_of_range("Page out of range");
  }

  BufferPool &bp = getDatabase().getBufferPool();
  Page &page = bp.getPage(PageId{name, it.page});
  HeapPage hp(page, td);

  if (hp.empty(it.slot)) {
    throw std::runtime_error("Slot is empty");
  }

  hp.deleteTuple(it.slot);
  bp.markDirty(PageId{name, it.page});
}


// Retrieve the tuple
Tuple HeapFile::getTuple(const Iterator &it) const {
  if (it.page >= numPages) {
    throw std::out_of_range("Page out of range");
  }

  BufferPool &bp = getDatabase().getBufferPool();
  Page &page = bp.getPage(PageId{name, it.page});
  HeapPage hp(page, td);

  return hp.getTuple(it.slot);
}


void HeapFile::next(Iterator &it) const {
  if (it.page >= numPages) {
    throw std::out_of_range("Iterator is beyond the file");
  }

  BufferPool &bp = getDatabase().getBufferPool();
  Page &page = bp.getPage(PageId{name, it.page});
  HeapPage hp(page, td);

  hp.next(it.slot);
  if (it.slot == hp.end()) {
    ++it.page;
    it.slot = 0;
  }
}


// Return an iterator to the first valid tuple in the file
Iterator HeapFile::begin() const {
  BufferPool &bp = getDatabase().getBufferPool();

  // Traverse all pages to find the first non-empty slot in the first non-empty page
  for (size_t i = 0; i < numPages; ++i) {
    Page &page = bp.getPage(PageId{name, i});
    HeapPage hp(page, td);

    size_t firstSlot = hp.begin();  // Find the first non-empty slot in the page
    if (firstSlot != hp.end()) {
      return Iterator(*this, i, firstSlot);  // Return an iterator pointing to the first non-empty slot
    }
  }
  return end();  // If no tuples are found, return end
}

Iterator HeapFile::end() const {
  return Iterator(*this, numPages, 0);  // Return an iterator pointing to the end of the file
}





