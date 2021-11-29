## Associated blog post
https://medium.com/@kelvingakuo/the-physical-structure-of-an-sql-index-a246a4c8b2b5

## B+ tree construction

Usage of the B+ tree is split into three operations:

### 1. Constructing the tree
Before going further, you need to understand the following:
1. The structure of a binary search tree i.e. the nodes on the left are always smaller than the parent, and on the right are larger
2. How a binary search tree is traversed
3. How values are inserted into a B-Tree (https://www.youtube.com/watch?v=DqcZLulVJ0M)

To traverse, we need to understand the following. Since each node can have multiple keys and multiple children:
1. A node with *n* keys will have *(n + 1)* children
2. When comparing a value with the keys of the node:

	a) If the value is less than *keys[0]*, the child at position *0* will have the value

	b) If the value is greater than or equal to *keys[len(keys) - 1]*, the child at position *len(keys)* will have the value

	c) For other values, the value is in the child at position *i + 1*


Construction goes as follows:
1. Accept a dict *{col_value: "", tid: ""}* ```BPlusTree().insert()```
2. Traverse the tree to identify the node where the value should be entered ```BPlusTree().find_insertion_leaf```
3. Insert the value into the keys of the node
4. If:

	a) The node is not full, return the root node of the tree ```BPlusTreeNode().update_node_attrs()```. This is by traversing upwards until we find a node without a parent

	b) The node is full, we split it. The list of keys and list of children is split into two by *ceiling(tree_order / 2)* and two new nodes returned. The key that needs to be inserted into the parent AKA floated, is also returned. ```BPlusTreeNode().split_node()```. If:

		I) The node that's just been split didn't have a parent and no children, we create a new node as the parent (with the float key) to the two just created nodes

		II) The node that's just been split didn't have a parent but had children, we create a new parent node (with the float key) for the two just created nodes. 
		The children of the old node will still be referring to the old node as the parent. We change these references accordingly.

		III) The node had a parent, we replace that node from the list of the parent's children with the two newly created nodes. We make sure to place them exactly where the split node was. We append the float keys to the parent's list of keys


	** For nodes that are determined to be leaf nodes, we update the list of keys to be *{col_value: , tid: }* to be inserted. If not a leaf node, the key is just the *col_value*



### 2. Linkifying the leaf nodes
What we just constructed isn't quite a B+ tree. For it to be, the leaf nodes of the tree have to nodes of a doubly-linked list.

Once we've inserted all our data into the tree, we linkify it by passing it to the function ```linkify_leaf_nodes()```. The function does the following:

1. Extracts a list of the tree's leaves from left to right ```BPlusTreeNode().get_tree_leaves()```
2. For each of those leaves, create a ```DoublyLinkedNode()```. Basically, create a doubly-linked node where the value is the BPlusTreeNode that's a leaf node
3. Sequentially link each newly created node to each other. Then, add the first node as the *head* for a ```DoublyLinkedList()```
4. (I found it easier to) Create a new tree. This is simply by changing the references in each of the leaf node's parents to point to the newly created doubly-linked nodes, not the previously reference bplus tree nodes. For each of the newly created node, check if their predecessor is in the parent's list of children, then replace it with self

The tree returned at the end is the same as the one we'd created but the leaf nodes are part of a doubly linked list created as ```LinkifiedBPlusTree()```

### 3. Doing lookup
Lookup is only possible with the linkified tree. The process is similar to insertion:
1. Find the node where value should be inserted. This will usually already have the value ```LinkifiedBPlusTree().find_tree()```


We have implemented the following operations:

**Equal to** ```LinkifiedBPlusTree().equal_to()```

First, extract all the keys equal to the value in the node identified.

Now, the index won't always contain unique values. Different leaf nodes may have the same value duplicated. To take care of this, we traverse the list forward looking for matching values and stop as soon as they stop matching. We do the same backward.

Combine the three lists to return all matching rows

**Less than** ```LinkifiedBPlusTree().less_than()```

Inside the node we just identified, extract all the keys that are less than the value we're looking at. Then traverse the list backwards extracting all the other values

**Greater than** ```LinkifiedBPlusTree().greater_than()```

Inside the node we just identified, extract all the keys that are greater than the value we're looking at. Then traverse the list forward extracting all the other values

**Less than or equal to**

Get all values equal to, then combine a list of all the values less than

**Greater than or equal to**

Get all values equal to, then combine a list of all the values greater than