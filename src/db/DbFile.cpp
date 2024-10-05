#include <db/DbFile.hpp>
#include <stdexcept>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

using namespace db;

const TupleDesc &DbFile::getTupleDesc() const { return td; }

DbFile::DbFile(const std::string &name, const TupleDesc &td) : name(name), td(td) {
  // TODO pa2: open file and initialize numPages
  fd = open(name.c_str(), O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
  if (fd == -1) {
    throw std::runtime_error("Unable to open the file: " + name);
  }


  struct stat fileStat;
  if (fstat(fd, &fileStat) == -1) {
    throw std::runtime_error("Unable to get file status: " + name);
  }

  numPages = static_cast<size_t>(fileStat.st_size) / page.size();
  if (numPages == 0) {
    numPages = 1;
  }
}

DbFile::~DbFile() {
  if (close(fd) == -1) {
    throw std::runtime_error("Unable to close the file: " + name);
  }
}

const std::string &DbFile::getName() const { return name; }


void DbFile::readPage(Page &page, const size_t id) const {
  // TODO pa2: read page
  // Hint: use pread
  reads.push_back(id);

  if (id >= numPages) {
    throw std::out_of_range("Page number out of range");
  }


  ssize_t bytesRead = pread(fd, page.data(), page.size(), id * page.size());
  if (bytesRead == -1) {
    throw std::runtime_error("Failed to read the page: " + std::to_string(id));
  }
}

void DbFile::writePage(const Page &page, const size_t id) const {
  writes.push_back(id);

  if (id >= numPages) {
    throw std::out_of_range("Page number out of range");
  }

  ssize_t bytesWritten = pwrite(fd, page.data(), page.size(), id * page.size());
  if (bytesWritten == -1) {
    throw std::runtime_error("Failed to write page: " + std::to_string(id));
  }
}


const std::vector<size_t> &DbFile::getReads() const { return reads; }

const std::vector<size_t> &DbFile::getWrites() const { return writes; }

void DbFile::insertTuple(const Tuple &t) { throw std::runtime_error("Not implemented"); }

void DbFile::deleteTuple(const Iterator &it) { throw std::runtime_error("Not implemented"); }

Tuple DbFile::getTuple(const Iterator &it) const { throw std::runtime_error("Not implemented"); }

void DbFile::next(Iterator &it) const { throw std::runtime_error("Not implemented"); }

Iterator DbFile::begin() const { throw std::runtime_error("Not implemented"); }

Iterator DbFile::end() const { throw std::runtime_error("Not implemented"); }

size_t DbFile::getNumPages() const { return numPages; }
