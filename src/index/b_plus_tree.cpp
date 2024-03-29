#include "index/b_plus_tree.h"
#include <string>

#include "glog/logging.h"
#include "index/generic_key.h"
#include "page/index_roots_page.h"

/**
 * TODO: Student Implement
 */
BPlusTree::BPlusTree(index_id_t index_id, BufferPoolManager *buffer_pool_manager, const KeyManager &KM,
                     int leaf_max_size, int internal_max_size)
    : index_id_(index_id),
      buffer_pool_manager_(buffer_pool_manager),
      processor_(KM),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {
  Page* page = buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID);
  auto index_root_page = reinterpret_cast<IndexRootsPage*>(page);
  index_root_page->GetRootId(index_id,&root_page_id_);
  buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, true);
    if(leaf_max_size == UNDEFINED_SIZE)
      leaf_max_size_ = (PAGE_SIZE - LEAF_PAGE_HEADER_SIZE) / (processor_.GetKeySize() + sizeof(RowId));
    if(internal_max_size == UNDEFINED_SIZE)
      internal_max_size_ = (PAGE_SIZE - INTERNAL_PAGE_HEADER_SIZE) / (processor_.GetKeySize() + sizeof(page_id_t));
}

void BPlusTree::Destroy(page_id_t current_page_id) {
  // 删除页
  buffer_pool_manager_->DeletePage(current_page_id);
}

/*
 * Helper function to decide whether current b+tree is empty
 */
bool BPlusTree::IsEmpty() const { return root_page_id_ == INVALID_PAGE_ID; }

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
bool BPlusTree::GetValue(const GenericKey *key, std::vector<RowId> &result, Transaction *transaction) {
  // 强制类型转换为leafPage处理
  if(root_page_id_==INVALID_PAGE_ID)return false;
  auto *leaf = reinterpret_cast<::LeafPage *>(FindLeafPage(key)->GetData());
  RowId ri;
  if (leaf->Lookup(key, ri, processor_)) {  // 找到
    buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
    result.push_back(ri);
    return true;
  } else {  // 没找到
    buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
    return false;
  }
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
bool BPlusTree::Insert(GenericKey *key, const RowId &value, Transaction *transaction) {
  if (IsEmpty()) {
    StartNewTree(key, value);
    return true;
  }
  return InsertIntoLeaf(key, value, transaction);
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
void BPlusTree::StartNewTree(GenericKey *key, const RowId &value) {
  // 创建新页
  page_id_t pageId;
  Page *page = buffer_pool_manager_->NewPage(pageId);
  if (page == nullptr) throw("out of memory in SNT");

  // 在新根上操作
  root_page_id_ = pageId;
  UpdateRootPageId(1);
  auto *rootPage = reinterpret_cast<::LeafPage *>(page->GetData());
  rootPage->Init(pageId, INVALID_PAGE_ID, processor_.GetKeySize(), leaf_max_size_);
  rootPage->Insert(key, value, processor_);
  buffer_pool_manager_->UnpinPage(root_page_id_, true);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exists, return
 * immediately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
bool BPlusTree::InsertIntoLeaf(GenericKey *key, const RowId &value, Transaction *transaction) {
  // 强制类型转换找到leaf去操作
  Page *page = FindLeafPage(key);
  auto *leaf = reinterpret_cast<::LeafPage *>(page->GetData());
  RowId ri;

  // 判断是否在leaf中，是则无法插入
  if (leaf->Lookup(key, ri, processor_)) {
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    return false;
  } else {  // 插入
    int size = leaf->Insert(key, value, processor_);
    if (size == leaf_max_size_) {  // 插入后需要分裂节点
      auto *recipient = Split(leaf, transaction);
      leaf->SetNextPageId(recipient->GetPageId());
      InsertIntoParent(leaf, recipient->KeyAt(0), recipient, transaction);
    }
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    return true;
  }
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
BPlusTreeInternalPage *BPlusTree::Split(InternalPage *node, Transaction *transaction) {
  // 开辟新页
  page_id_t pageId;
  Page *newPage = buffer_pool_manager_->NewPage(pageId);
  if (newPage == nullptr) throw("out of memory in SI");

  // 强制类型转换，操作recipient，注意初始化
  auto *recipient = reinterpret_cast<::InternalPage *>(newPage->GetData());
  recipient->Init(pageId, node->GetParentPageId(), node->GetKeySize(), internal_max_size_);
  node->MoveHalfTo(recipient, buffer_pool_manager_);
  buffer_pool_manager_->UnpinPage(pageId, true);
  return recipient;
}

BPlusTreeLeafPage *BPlusTree::Split(LeafPage *node, Transaction *transaction) {
  // 开辟新页
  page_id_t pageId;
  Page *newPage = buffer_pool_manager_->NewPage(pageId);
  if (newPage == nullptr) throw("out of memory in SL");

  // 强制类型转换，操作recipient，注意初始化
  auto *recipient = reinterpret_cast<::LeafPage *>(newPage->GetData());
  recipient->Init(pageId, node->GetParentPageId(), node->GetKeySize(), leaf_max_size_);
  node->MoveHalfTo(recipient);
  recipient->SetNextPageId(node->GetNextPageId());
  buffer_pool_manager_->UnpinPage(pageId, true);
  return recipient;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
void BPlusTree::InsertIntoParent(BPlusTreePage *old_node, GenericKey *key, BPlusTreePage *new_node,
                                 Transaction *transaction) {
  if (old_node->IsRootPage()) {  // 被分裂的节点是根节点，需要建立新根节点
    // 新建页
    page_id_t rootId;
    Page *page = buffer_pool_manager_->NewPage(rootId);
    auto *root = reinterpret_cast<BPlusTree::InternalPage *>(page->GetData());

    // 初始化新根
    root->Init(rootId, INVALID_PAGE_ID, old_node->GetKeySize(), internal_max_size_);
    root->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
    old_node->SetParentPageId(rootId);
    new_node->SetParentPageId(rootId);
    root_page_id_ = rootId;
    UpdateRootPageId(0);
    buffer_pool_manager_->UnpinPage(rootId, true);
    return;
  } else {  // 被分裂的节点不是根节点，向上迭代
    Page *page = buffer_pool_manager_->FetchPage(old_node->GetParentPageId());
    auto *parent = reinterpret_cast<BPlusTree::InternalPage *>(page->GetData());

    // 操作被分裂节点的父节点，插入新分裂节点的pageId
    int size = parent->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
    // 判断是否需要继续向上迭代
    if (size == internal_max_size_) {
      // 分裂出父亲的兄弟
      auto *rParent = Split(parent, transaction);
      // 迭代向上
      InsertIntoParent(parent, rParent->KeyAt(0), rParent, transaction);
    }
    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
    return;
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
void BPlusTree::Remove(const GenericKey *key, Transaction *transaction) {
  // 判断空
  if (IsEmpty()) return;

  // 找到叶节点
  Page *page = FindLeafPage(key);
  auto *leaf = reinterpret_cast<::LeafPage *>(page->GetData());

  // 删除键值对
  bool judge = leaf->KeyIndex(key, processor_) == 0;
  int size = leaf->RemoveAndDeleteRecord(key, processor_);
  bool deleted = false;
  if (size < leaf->GetMinSize())
    deleted = CoalesceOrRedistribute<BPlusTree::LeafPage>(leaf, transaction);
  else if (judge) {  // 直接删除后调整键值对
    GenericKey *updateKey = leaf->KeyAt(0);
    Page *currPage = buffer_pool_manager_->FetchPage(leaf->GetParentPageId());
    auto curr = reinterpret_cast<::InternalPage *>(currPage);
    page_id_t value = leaf->GetPageId();
    if(currPage!= nullptr){//当前删除叶节点不是根节点
      while ((!curr->IsRootPage()) && curr->ValueIndex(value) == 0) {  // 对应key0一直上滤到根节点
        buffer_pool_manager_->UnpinPage(curr->GetPageId(), false);
        value = curr->GetPageId();
        currPage = buffer_pool_manager_->FetchPage(curr->GetParentPageId());
        curr = reinterpret_cast<::InternalPage *>(currPage);
      }
      if (curr->ValueIndex(value) != 0 && processor_.CompareKeys(updateKey, curr->KeyAt(curr->ValueIndex(value))) != 0) {
        curr->SetKeyAt(curr->ValueIndex(value), updateKey);
        buffer_pool_manager_->UnpinPage(curr->GetPageId(), true);
      }
    }
  }
  // unpin后才有可能删除
  buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
  if (deleted) buffer_pool_manager_->DeletePage(page->GetPageId());
}

/* todo
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted,
 * false means no deletion happens       ???猜测只是没有删这个leaf page的意思
 */
template <typename N>
bool BPlusTree::CoalesceOrRedistribute(N *&node, Transaction *transaction) {
  // 直接调整根
  if (node->IsRootPage()) {
    return AdjustRoot(node);
  }

  // 从node的父亲处获得node的index
  int pre_index, next_index, node_index;
  page_id_t parent_id, pre_id, node_id, next_id;
  parent_id = node->GetParentPageId();
  node_id = node->GetPageId();
  Page *temp;
  BPlusTree::InternalPage *parent;
  N *pre;
  N *next;
  parent = reinterpret_cast<BPlusTree::InternalPage *>(buffer_pool_manager_->FetchPage(parent_id)->GetData());
  node_index = parent->ValueIndex(node->GetPageId());

  // redistribute
  //  判断能否和pre节点redistribute
  if (node_index > 0) {  // 即不是第一个节点
    pre_index = node_index - 1;
    pre_id = parent->ValueAt(pre_index);
    pre = reinterpret_cast<N *>(buffer_pool_manager_->FetchPage(pre_id)->GetData());
    // 从pre移动一个键值重建node
    if (pre->GetSize() > pre->GetMinSize()) {  // 至少可以移动一个键值对
      Redistribute(pre, node, 1);
      auto child = reinterpret_cast<BPlusTreePage *>(node);
      if (!child->IsLeafPage()) {
        auto child_internal = reinterpret_cast<::InternalPage *>(child);
        page_id_t answer_id = child_internal->LeftMostKeyFromCurr(buffer_pool_manager_);
        Page* leaf_page = buffer_pool_manager_->FetchPage(answer_id);
        auto leaf = reinterpret_cast<::LeafPage*>(leaf_page);
        parent->SetKeyAt(node_index,leaf->KeyAt(0));
        buffer_pool_manager_->UnpinPage(leaf->GetPageId(),false);
      }
      buffer_pool_manager_->UnpinPage(parent_id, true);
      buffer_pool_manager_->UnpinPage(pre_id, true);
      return false;
    }
  }
  // 判断能否和next节点redistribute
  if (node_index < parent->GetSize() - 1) {  // 情况包含第一个节点，以及无法和pre节点redistribute
    next_index = node_index + 1;
    next_id = parent->ValueAt(next_index);
    next = reinterpret_cast<N *>(buffer_pool_manager_->FetchPage(next_id)->GetData());
    // 从next移动一个键值重建node
    if (next->GetSize() > next->GetMinSize()) {  // 至少可以移动一个键值对
      Redistribute(next, node, 0);
      auto child = reinterpret_cast<BPlusTreePage *>(next);
      if (!child->IsLeafPage()) {
        auto child_internal = reinterpret_cast<::InternalPage *>(child);
        page_id_t answer_id = child_internal->LeftMostKeyFromCurr(buffer_pool_manager_);
        Page* leaf_page = buffer_pool_manager_->FetchPage(answer_id);
        auto leaf = reinterpret_cast<::LeafPage*>(leaf_page);
        parent->SetKeyAt(next_index,leaf->KeyAt(0));
        buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
      }
      buffer_pool_manager_->UnpinPage(parent_id, true);
      buffer_pool_manager_->UnpinPage(next_id, true);
      return false;
    }
  }

  // coalesce
  if (node_index > 0) {  // 不是第一个，肯定用不到next_id
    buffer_pool_manager_->UnpinPage(next_id, false);
    Coalesce(pre, node, parent, node_index, transaction);
    buffer_pool_manager_->UnpinPage(node_id, true);
    buffer_pool_manager_->UnpinPage(parent_id, true);
    return true;
  } else {
    buffer_pool_manager_->UnpinPage(pre_id, false);
    Coalesce(node, next, parent, next_index, transaction);
    buffer_pool_manager_->UnpinPage(parent_id, true);
    buffer_pool_manager_->UnpinPage(next_id, true);
    return false;
  }
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion happened
 */
bool BPlusTree::Coalesce(LeafPage *&neighbor_node, LeafPage *&node, InternalPage *&parent, int index,
                         Transaction *transaction) {
  if (neighbor_node->GetNextPageId() == 0) LOG(INFO) << "Fatal error in Coalesce";
  node->MoveAllTo(neighbor_node);
  parent->Remove(index);
  if (parent->GetSize() < parent->GetMinSize())
    return CoalesceOrRedistribute<BPlusTree::InternalPage>(parent, transaction);
  else
    return false;
}

bool BPlusTree::Coalesce(InternalPage *&neighbor_node, InternalPage *&node, InternalPage *&parent, int index,
                         Transaction *transaction) {
  node->MoveAllTo(neighbor_node, parent->KeyAt(index), buffer_pool_manager_);
  parent->Remove(index);
  if (parent->GetSize() < parent->GetMinSize())
    return CoalesceOrRedistribute<BPlusTree::InternalPage>(parent, transaction);
  else
    return false;
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
void BPlusTree::Redistribute(LeafPage *neighbor_node, LeafPage *node, int index) {
  // 获取node父亲指针
  page_id_t parent = node->GetParentPageId();
  auto *parent_ptr = reinterpret_cast<BPlusTree::InternalPage *>(buffer_pool_manager_->FetchPage(parent)->GetData());

  if (index == 0) {  // FTE
    neighbor_node->MoveFirstToEndOf(node);
    int node_index = parent_ptr->ValueIndex(neighbor_node->GetPageId());
    parent_ptr->SetKeyAt(node_index, neighbor_node->KeyAt(0));
  } else {  // LTF
    neighbor_node->MoveLastToFrontOf(node);
    int node_index = parent_ptr->ValueIndex(node->GetPageId());
    parent_ptr->SetKeyAt(node_index, node->KeyAt(0));
  }
  buffer_pool_manager_->UnpinPage(parent, true);
}
void BPlusTree::Redistribute(InternalPage *neighbor_node, InternalPage *node, int index) {
  // 获取node父亲指针
  page_id_t parent = node->GetParentPageId();
  auto *parent_ptr = reinterpret_cast<BPlusTree::InternalPage *>(buffer_pool_manager_->FetchPage(parent)->GetData());

  if (index == 0) {  // FTE
    int neighbor_index = parent_ptr->ValueIndex(neighbor_node->GetPageId());
    GenericKey *newKey = neighbor_node->KeyAt(1);
    neighbor_node->MoveFirstToEndOf(node, parent_ptr->KeyAt(neighbor_index), buffer_pool_manager_);
    parent_ptr->SetKeyAt(neighbor_index, newKey);
  } else {  // LTF
    int node_index = parent_ptr->ValueIndex(node->GetPageId());
    GenericKey *newKey = neighbor_node->KeyAt(neighbor_node->GetSize() - 1);
    neighbor_node->MoveLastToFrontOf(node, parent_ptr->KeyAt(node_index), buffer_pool_manager_);
    parent_ptr->SetKeyAt(node_index, newKey);
  }
  buffer_pool_manager_->UnpinPage(parent, true);
}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happened
 */
bool BPlusTree::AdjustRoot(BPlusTreePage *old_root_node) {
  if (old_root_node->GetSize() > 1)  // 节点大于二，不用删除
    return false;
  if (old_root_node->IsLeafPage()) {
    if (old_root_node->GetSize() == 1) return false;  // root同时是leaf，节点不为0即不用删除
    root_page_id_ = INVALID_PAGE_ID;
    UpdateRootPageId(0);
    return true;
  }

  auto root = reinterpret_cast<BPlusTree::InternalPage *>(old_root_node);
  root_page_id_ = root->RemoveAndReturnOnlyChild();
  auto new_root_node =
      reinterpret_cast<BPlusTree::InternalPage *>(buffer_pool_manager_->FetchPage(root_page_id_)->GetData());
  new_root_node->SetParentPageId(INVALID_PAGE_ID);
  buffer_pool_manager_->UnpinPage(root_page_id_, true);
  UpdateRootPageId(0);
  return true;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the left most leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
IndexIterator BPlusTree::Begin() {
  Page *page = FindLeafPage(nullptr, INVALID_PAGE_ID, true);
  if (page == nullptr) return IndexIterator();
  int page_id = page->GetPageId();
  buffer_pool_manager_->UnpinPage(page_id, false);
  return IndexIterator(page_id, buffer_pool_manager_);
}

/*
 * Input parameter is low-key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
IndexIterator BPlusTree::Begin(const GenericKey *key) {
  Page *page = FindLeafPage(key, INVALID_PAGE_ID, false);
  if (page == nullptr) return IndexIterator();
  int page_id = page->GetPageId();
  auto *leaf = reinterpret_cast<BPlusTree::LeafPage *>(page);
  buffer_pool_manager_->UnpinPage(page_id, false);
  return IndexIterator(page_id, buffer_pool_manager_, leaf->KeyIndex(key, processor_));
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
IndexIterator BPlusTree::End() {
  return IndexIterator();
//  auto *page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(root_page_id_)->GetData());
//  if (page == nullptr) return IndexIterator();
//  page_id_t pageId = page->GetPageId();
//  while (!page->IsLeafPage()) {
//    auto *internalpage = reinterpret_cast<BPlusTree::InternalPage *>(page);
//    pageId = internalpage->ValueAt(internalpage->GetSize() - 1);
//    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
//    page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(pageId)->GetData());
//  }
//  auto *leaf = reinterpret_cast<BPlusTree::LeafPage *>(page);
//  buffer_pool_manager_->UnpinPage(pageId, false);
//  return IndexIterator(pageId, buffer_pool_manager_, leaf->GetSize() - 1);
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 * Note: the leaf page is pinned, you need to unpin it after use.
 */
Page *BPlusTree::FindLeafPage(const GenericKey *key, page_id_t page_id, bool leftMost) {
  if (IsEmpty()) return nullptr;

  // 当前页从b+树根节点开始
  Page *currPage = buffer_pool_manager_->FetchPage(root_page_id_);
  page_id_t currPageId = root_page_id_;
  auto *curr = reinterpret_cast<BPlusTreePage *>(currPage->GetData());

  // 向下寻找直到叶节点
  while (!curr->IsLeafPage()) {
    buffer_pool_manager_->UnpinPage(currPageId, false);  // 每找一层关闭上一层的内节点page
    auto *internalPage = reinterpret_cast<BPlusTree::InternalPage *>(currPage);  // 打开上一层内节点page
    if (leftMost)                                                                // 一路向左
      currPageId = internalPage->ValueAt(0);
    else
      currPageId = internalPage->Lookup(key, processor_);

    currPage = buffer_pool_manager_->FetchPage(currPageId);  // 改变当前页的指针
    curr = reinterpret_cast<BPlusTreePage *>(currPage->GetData());
  }
  // 在GetValue()中unpin
  return currPage;
}

/*
 * Update/Insert root page id in IndexRootsPage(where page_id = 0, index_roots__page is
 * defined under include/page/index_roots__page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      default value is false. When set to true,
 * insert a record <index_name, current_page_id> into header page instead of
 * updating it.
 */
void BPlusTree::UpdateRootPageId(int insert_record) {
  auto *index_roots_page = reinterpret_cast<IndexRootsPage *>(buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_id_ + root_page_id> in index_roots_page
    index_roots_page->Insert(index_id_, root_page_id_);
  } else {
    // update root_page_id in index_roots_page
    index_roots_page->Update(index_id_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, true);
}

/**
 * This method is used for debug only, You don't need to modify
 */
void BPlusTree::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId()
        << ",Parent=" << leaf->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId()
        << ",Parent=" << inner->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 */
void BPlusTree::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
      bpm->UnpinPage(internal->ValueAt(i), false);
    }
  }
}

bool BPlusTree::Check() {
  bool all_unpinned = buffer_pool_manager_->CheckAllUnpinned();
  if (!all_unpinned) {
    LOG(ERROR) << "problem in page unpin" << endl;
  }
  return all_unpinned;
}