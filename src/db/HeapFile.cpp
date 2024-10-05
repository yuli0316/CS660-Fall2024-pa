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

  std::vector<Page> pages;

  size_t currentNumPages = getNumPages();

  for (size_t i = 0; i < currentNumPages; ++i) {
    Page page;

    page = getDatabase().getBufferPool().getPage(PageId{name, i});

    HeapPage hp(page, getTupleDesc());

    for (size_t slot = hp.begin(); slot != hp.end(); ++slot) {
      if (hp.empty(slot)) {
        if(hp.insertTuple(t)) {
          getDatabase().getBufferPool().markDirty(PageId{name, i});
          pages.push_back(page);
          return;
        }
      }
    }
  }

  Page newPage;
  HeapPage newHp(newPage, getTupleDesc());

  if (!newHp.insertTuple(t)) {
    throw std::runtime_error("Failed to insert tuple into a new page");
  }

  writePage(newPage, currentNumPages);
  getDatabase().getBufferPool().markDirty(PageId{name, currentNumPages});

  ++numPages;
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


Iterator HeapFile::begin() const {
  BufferPool &bp = getDatabase().getBufferPool();

  for (size_t i = 0; i < numPages; ++i) {
    Page &page = bp.getPage(PageId{name, i});
    HeapPage hp(page, td);

    size_t firstSlot = hp.begin();
    if (firstSlot != hp.end()) {
      return Iterator(*this, i, firstSlot);
    }
  }
  return end();
}

Iterator HeapFile::end() const {
  return Iterator(*this, numPages, 0);
}





