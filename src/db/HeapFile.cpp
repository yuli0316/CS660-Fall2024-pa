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

  // Retrieve the current number of pages
  size_t currentNumPages = getNumPages();

  // Traverse through the pages in the file
  for (size_t i = 0; i < currentNumPages; ++i) {
    Page page;
    // Fetch the page from BufferPool (or read from the file)
    page = getDatabase().getBufferPool().getPage(PageId{name, i});

    // Create a HeapPage object to operate on the page
    HeapPage hp(page, getTupleDesc());

    // Attempt to insert the tuple. If successful, mark the page as dirty and return.
    if (hp.insertTuple(t)) {
      getDatabase().getBufferPool().markDirty(PageId{name, i});
      return;
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
  writePage(newPage, currentNumPages);
  getDatabase().getBufferPool().markDirty(PageId{name, currentNumPages});

  // Increment the page count after adding the new page
  ++numPages;
}



// Delete the tuple
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





