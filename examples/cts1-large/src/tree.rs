//! A B+ tree implementation where every node is a smart contract and the leafs
//! are pointing to contracts containing the keys.
//! In this example the keys are `Address`.
//!
//! Another smart contract will be the entry to the B+ tree, which will receive
//! the initial calls and track the current root of the tree. The entry node
//! also contains a list of nodes for future use, that can be requested by nodes
//! and leafs, when doing a split.
//!
//! Some further development could be to workaround the limit on the parameter
//! size, and send multiple messages with data when doing a split, instead of
//! just the one message which is currently limiting the size of each node.

#![cfg_attr(not(feature = "std"), no_std)]
use concordium_std::*;

const BRANCHING_FACTOR: usize = 320;
const NODE_CAPACITY: usize = BRANCHING_FACTOR - 1;
const SPLIT_INDEX: usize = BRANCHING_FACTOR / 2;

// Types

type Key = Address;
type Value = ContractAddress;
type ChildPointer = ContractAddress;

#[derive(Serialize, SchemaType)]
struct Branch {
    /// The smallest key found in this branch.
    /// Is none if this is the first/smallest branch
    min:   Option<Key>,
    child: ChildPointer,
}

#[derive(Serialize, SchemaType)]
struct KeyValue {
    key:   Key,
    value: Value,
}

#[derive(Serialize, SchemaType)]
enum InternalNodeStatus {
    // ReceivingBranches
    Ready,
    AwaitingNodes,
}

#[derive(Serialize, SchemaType)]
struct InternalNode(Vec<Branch>, InternalNodeStatus);

#[derive(Serialize, SchemaType)]
enum LeafNodeStatus {
    // ReceivingKeyValues
    Ready,
    AwaitingNode,
}

#[derive(Serialize, SchemaType)]
struct LeafNode(Vec<KeyValue>, LeafNodeStatus);

#[derive(Serialize, SchemaType)]
enum Node {
    NotActive,
    Internal(InternalNode),
    Leaf(LeafNode),
}

enum InsertResult {
    Done,
    Split,
}

// Contract function parameters

#[derive(Serialize, SchemaType)]
struct ActivateNodeParams {
    parent: Option<ContractAddress>,
    node:   Node,
}

#[derive(Serialize, SchemaType)]
struct AddNodesParams {
    nodes: Vec<ContractAddress>,
}

#[derive(Serialize, SchemaType)]
struct SearchParams {
    key:               Key,
    callback_contract: ContractAddress,
    callback_function: OwnedReceiveName,
}

#[derive(Serialize, SchemaType)]
struct TrustParams {
    address: ContractAddress,
}

#[derive(Serialize, SchemaType)]
struct AvailableNodeParams {
    address: ContractAddress,
    root:    Option<ContractAddress>,
}

#[derive(Serialize, SchemaType)]
struct InsertParams {
    key:               Key,
    value:             Value,
    callback_contract: ContractAddress,
    callback_function: OwnedReceiveName,
}

#[derive(Serialize, SchemaType)]
struct InsertBranchParams {
    branch: Branch,
}

// Contract state

#[derive(Serialize, SchemaType)]
struct State {
    entry:   ContractAddress,
    /// Additional contract address which is trusted.
    /// Used during split.
    trusted: Option<ContractAddress>,
    /// Address of the parent node.
    /// None if inactive or the root node
    parent:  Option<ContractAddress>,
    node:    Node,
}

#[derive(Serialize, SchemaType)]
struct EntryState {
    root_node:       Option<ContractAddress>,
    //authorized: Vec<Address>
    available_nodes: Vec<ContractAddress>,
}

impl InternalNode {
    fn search(&self, key: &Key) -> &ChildPointer {
        let InternalNode(branches, _) = self;
        let result = branches.binary_search_by_key(&Some(*key), |b| b.min);
        let index = match result {
            Ok(i) => i,
            Err(i) => i,
        };
        &branches[index].child
    }

    fn insert_branch(&mut self, branch: Branch) -> InsertResult {
        let InternalNode(branches, _) = self;
        let result = branches.binary_search_by_key(&branch.min, |b| b.min);
        let index = match result {
            Ok(i) => i,
            Err(i) => i,
        };
        branches.insert(index, branch);
        if branches.len() <= NODE_CAPACITY {
            InsertResult::Done
        } else {
            InsertResult::Split
        }
    }

    fn split(&mut self) -> Self {
        let InternalNode(branches, _) = self;
        let mut new_internal_node_branches = branches.split_off(SPLIT_INDEX);
        new_internal_node_branches[0].min = None;
        InternalNode(new_internal_node_branches, InternalNodeStatus::Ready)
    }

    fn min_key(&self) -> Option<Key> {
        let InternalNode(branches, _) = self;
        branches.get(1).and_then(|b| b.min)
    }

    fn status(&self) -> &InternalNodeStatus {
        let InternalNode(_, status) = self;
        &status
    }

    fn set_status(&mut self, new_status: InternalNodeStatus) {
        let InternalNode(_, status) = self;
        *status = new_status
    }

    fn is_child_branch(&self, address: &ContractAddress) -> bool {
        let InternalNode(branches, _) = self;
        branches.binary_search_by_key(address, |b| b.child).is_ok()
    }
}

impl LeafNode {
    fn search(&self, key: &Key) -> Option<Value> {
        let LeafNode(key_values, _) = self;
        key_values
            .binary_search_by_key(key, |c| c.key)
            .ok()
            .and_then(|i| key_values.get(i))
            .map(|kv| kv.value)
    }

    fn insert_key_value(&mut self, key: Key, value: Value) -> InsertResult {
        let LeafNode(key_values, _) = self;
        let result = key_values.binary_search_by_key(&key, |c| c.key);
        let index = match result {
            Ok(i) => i,
            Err(i) => i,
        };
        key_values.insert(index, KeyValue {
            value,
            key,
        });
        if key_values.len() <= NODE_CAPACITY {
            InsertResult::Done
        } else {
            InsertResult::Split
        }
    }

    fn split(&mut self) -> Self {
        let LeafNode(key_values, _) = self;
        let new_leaf_key_values = key_values.split_off(SPLIT_INDEX);
        LeafNode(new_leaf_key_values, LeafNodeStatus::Ready)
    }

    fn min_key(&self) -> Option<Key> {
        let LeafNode(key_values, _) = self;
        key_values.get(0).map(|kv| kv.key)
    }

    fn status(&self) -> &LeafNodeStatus { &self.1 }

    fn set_status(&mut self, status: LeafNodeStatus) { self.1 = status }
}

// The `entry` smart contract functions

/// Initialize a entry contract, a tree will only have one entry.
#[init(contract = "entry")]
fn contract_entry_init(_ctx: &impl HasInitContext) -> InitResult<EntryState> {
    Ok(EntryState {
        root_node:       None,
        available_nodes: Vec::new(),
    })
}

/// Add nodes to available nodes, assumes these node contracts are instances of
/// the `node` contract.
#[receive(contract = "entry", name = "addNodes")]
fn contract_entry_add_nodes<A: HasActions>(
    ctx: &impl HasReceiveContext,
    state: &mut EntryState,
) -> ReceiveResult<A> {
    ensure!(ctx.sender().matches_account(&ctx.owner()));
    let params: AddNodesParams = ctx.parameter_cursor().get()?;
    state.available_nodes.extend(params.nodes);
    Ok(A::accept())
}

/// Search the tree
#[receive(contract = "entry", name = "search")]
fn contract_entry_search<A: HasActions>(
    ctx: &impl HasReceiveContext,
    state: &mut EntryState,
) -> ReceiveResult<A> {
    let root = if let Some(root) = state.root_node {
        root
    } else {
        bail!()
    };
    let params: SearchParams = ctx.parameter_cursor().get()?;
    Ok(send(&root, ReceiveName::new_unchecked("node.search"), Amount::from_micro_gtu(0), &params))
}

/// Insert new key value into the tree
///
/// If no root (meaning it is the first key) it creates the first leaf and
/// internal node.
#[receive(contract = "entry", name = "insert")]
fn contract_entry_insert<A: HasActions>(
    ctx: &impl HasReceiveContext,
    state: &mut EntryState,
) -> ReceiveResult<A> {
    ensure!(ctx.sender().matches_account(&ctx.owner()));
    let params: InsertParams = ctx.parameter_cursor().get()?;
    if let Some(root) = state.root_node {
        return Ok(send(
            &root,
            ReceiveName::new_unchecked("node.insert"),
            Amount::from_micro_gtu(0),
            &params,
        ));
    }

    let root = state.available_nodes.pop().ok_or(Reject::default())?;
    let parameter = ActivateNodeParams {
        parent: None,
        node:   Node::Internal(InternalNode(Vec::new(), InternalNodeStatus::Ready)),
    };
    let become_root_action: A = send(
        &root,
        ReceiveName::new_unchecked("node.activate"),
        Amount::from_micro_gtu(0),
        &parameter,
    );

    let leaf = state.available_nodes.pop().ok_or(Reject::default())?;
    let mut key_values = Vec::new();
    key_values.push(KeyValue {
        key:   params.key,
        value: params.value,
    });
    let parameter = ActivateNodeParams {
        parent: Some(root),
        node:   Node::Leaf(LeafNode(key_values, LeafNodeStatus::Ready)),
    };
    let become_leaf_action = send(
        &leaf,
        ReceiveName::new_unchecked("node.activate"),
        Amount::from_micro_gtu(0),
        &parameter,
    );

    let parameter = InsertBranchParams {
        branch: Branch {
            min:   None,
            child: leaf,
        },
    };
    let insert_leaf_action = send(
        &root,
        ReceiveName::new_unchecked("node.insertBranch"),
        Amount::from_micro_gtu(0),
        &parameter,
    );

    Ok(become_root_action.and_then(become_leaf_action).and_then(insert_leaf_action))
}

/// Tell an available node to trust the sender and send back the address of the
/// available node to the sender. If the sender is the root node, it will return
/// two new nodes, where one of them is set as the new root.
#[receive(contract = "entry", name = "requestNode")]
fn contract_entry_request_node<A: HasActions>(
    ctx: &impl HasReceiveContext,
    state: &mut EntryState,
) -> ReceiveResult<A> {
    // TODO: Authentication
    let node = state.available_nodes.pop().ok_or(Reject::default())?;
    let sender = if let Address::Contract(address) = ctx.sender() {
        address
    } else {
        bail!()
    };
    let trust_params = TrustParams {
        address: sender,
    };
    let trust_action: A = send(
        &node,
        ReceiveName::new_unchecked("node.trust"),
        Amount::from_micro_gtu(0),
        &trust_params,
    );
    let (root, root_trust_action) = if Some(sender) == state.root_node {
        let node = state.available_nodes.pop().ok_or(Reject::default())?;
        state.root_node = Some(node);
        (
            Some(node),
            send(
                &node,
                ReceiveName::new_unchecked("node.trust"),
                Amount::from_micro_gtu(0),
                &trust_params,
            ),
        )
    } else {
        (None, A::accept())
    };

    let available_node_params = AvailableNodeParams {
        address: node,
        root,
    };
    let send_node_action = send(
        &sender,
        ReceiveName::new_unchecked("node.availableNode"),
        Amount::from_micro_gtu(0),
        &available_node_params,
    );
    Ok(trust_action.and_then(root_trust_action).and_then(send_node_action))
}

// The `node` smart contract functions,

/// Initialize a node contract
#[init(contract = "node")]
fn contract_node_init(ctx: &impl HasInitContext) -> InitResult<State> {
    let entry = ctx.parameter_cursor().get()?;
    Ok(State {
        entry,
        trusted: None,
        parent: None,
        node: Node::NotActive,
    })
}

/// Search the tree for which contract holds the value
#[receive(contract = "node", name = "search")]
fn contract_node_search<A: HasActions>(
    ctx: &impl HasReceiveContext,
    state: &mut State,
) -> ReceiveResult<A> {
    let params: SearchParams = ctx.parameter_cursor().get()?;

    match &state.node {
        Node::Internal(node) => {
            let next_node = node.search(&params.key);
            Ok(send(
                next_node,
                ReceiveName::new_unchecked("node.search"),
                Amount::from_micro_gtu(0),
                &params,
            ))
        }
        Node::Leaf(node) => {
            let value = node.search(&params.key);
            Ok(send(
                &params.callback_contract,
                params.callback_function.as_ref(),
                Amount::from_micro_gtu(0),
                &value,
            ))
        }
        _ => bail!(),
    }
}

/// Insert new key value.
/// Will reject if not called by entry or the parent.
///
/// For internal nodes it will forward the insert the right branch
/// For leaf nodes it will insert the key value, and if overflow it will request
/// a new node to perform a split.
#[receive(contract = "node", name = "insert")]
fn contract_node_insert<A: HasActions>(
    ctx: &impl HasReceiveContext,
    state: &mut State,
) -> ReceiveResult<A> {
    let sender = if let Address::Contract(address) = ctx.sender() {
        address
    } else {
        bail!()
    };
    ensure!(sender == state.entry || matches!(state.parent, Some(parent) if parent == sender));

    let params: InsertParams = ctx.parameter_cursor().get()?;

    match &mut state.node {
        Node::Internal(node) => {
            let next_node = node.search(&params.key);
            Ok(send(
                next_node,
                ReceiveName::new_unchecked("node.insert"),
                Amount::from_micro_gtu(0),
                &params,
            ))
        }
        Node::Leaf(node) => {
            let result = node.insert_key_value(params.key, params.value);
            if let InsertResult::Split = result {
                node.set_status(LeafNodeStatus::AwaitingNode);
                Ok(A::send_raw(
                    &state.entry,
                    ReceiveName::new_unchecked("entry.requestNode"),
                    Amount::from_micro_gtu(0),
                    &[],
                ))
            } else {
                Ok(A::accept())
            }
        }
        _ => bail!(),
    }
}

/// Tell a node to trust messages from another node, will reject if not send by
/// `entry` contract. This is used to authorize a `node` contract to activate
/// available nodes when performing a split.
#[receive(contract = "node", name = "trust")]
fn contract_node_trust<A: HasActions>(
    ctx: &impl HasReceiveContext,
    state: &mut State,
) -> ReceiveResult<A> {
    ensure!(matches!(state.node, Node::NotActive));
    ensure!(matches!(ctx.sender(), Address::Contract(address) if address == state.entry));
    let params: TrustParams = ctx.parameter_cursor().get()?;
    state.trusted = Some(params.address);
    Ok(A::accept())
}

/// Receive available nodes and perform a split.
/// This is called by the entry contract, after a node have request nodes.
///
/// For a Leaf node it will send a message to the available node to become a
/// leaf with half of the keyvalues. For an Internal node, it will tell the
/// available node to become an internal node with half of the branches,
/// additionally if this is the root node it will send a message to the new root
/// node with itself and the other internal node as the branches.
#[receive(contract = "node", name = "availableNode")]
fn contract_node_available_node<A: HasActions>(
    ctx: &impl HasReceiveContext,
    state: &mut State,
) -> ReceiveResult<A> {
    ensure!(ctx.sender().matches_contract(&state.entry));
    let params: AvailableNodeParams = ctx.parameter_cursor().get()?;
    match &mut state.node {
        Node::Internal(node) => match node.status() {
            InternalNodeStatus::AwaitingNodes => {
                let new_node = node.split();
                let min = new_node.min_key();
                let parent = state.parent.or(params.root).ok_or(Reject::default())?;
                let parameter = ActivateNodeParams {
                    parent: Some(parent),
                    node:   Node::Internal(new_node),
                };
                let become_internal_node_action = send(
                    &params.address,
                    ReceiveName::new_unchecked("node.activate"),
                    Amount::from_micro_gtu(0),
                    &parameter,
                );

                let parameter = InsertBranchParams {
                    branch: Branch {
                        min,
                        child: params.address,
                    },
                };
                let insert_internal_node_action = send(
                    &parent,
                    ReceiveName::new_unchecked("node.insertBranch"),
                    Amount::from_micro_gtu(0),
                    &parameter,
                );

                let become_root_action = if let Some(root) = params.root {
                    let mut branches = Vec::new();
                    branches.push(Branch {
                        min:   None,
                        child: ctx.self_address(),
                    });
                    branches.push(Branch {
                        min,
                        child: params.address,
                    });
                    let root_node = InternalNode(branches, InternalNodeStatus::Ready);
                    let parameter = ActivateNodeParams {
                        parent: None,
                        node:   Node::Internal(root_node),
                    };
                    send(
                        &root,
                        ReceiveName::new_unchecked("node.activate"),
                        Amount::from_micro_gtu(0),
                        &parameter,
                    )
                } else {
                    A::accept()
                };

                Ok(become_root_action
                    .and_then(become_internal_node_action)
                    .and_then(insert_internal_node_action))
            }
            _ => bail!(),
        },
        Node::Leaf(node) => {
            if let LeafNodeStatus::AwaitingNode = node.status() {
                let new_node = node.split();
                let min = new_node.min_key().ok_or(Reject::default())?;

                let parameter = ActivateNodeParams {
                    parent: state.parent,
                    node:   Node::Leaf(new_node),
                };
                node.set_status(LeafNodeStatus::Ready);
                let become_leaf_action: A = send(
                    &params.address,
                    ReceiveName::new_unchecked("node.activate"),
                    Amount::from_micro_gtu(0),
                    &parameter,
                );

                let parameter = InsertBranchParams {
                    branch: Branch {
                        min:   Some(min),
                        child: params.address,
                    },
                };
                let insert_leaf_action = send(
                    &state.parent.ok_or(Reject::default())?,
                    ReceiveName::new_unchecked("node.insertBranch"),
                    Amount::from_micro_gtu(0),
                    &parameter,
                );
                Ok(become_leaf_action.and_then(insert_leaf_action))
            } else {
                bail!()
            }
        }
        _ => bail!(),
    }
}

/// Activate the node to be either an internal node or leaf node.
/// Reject if not called by the entry or a trusted contract.
#[receive(contract = "node", name = "activate")]
fn contract_node_activate<A: HasActions>(
    ctx: &impl HasReceiveContext,
    state: &mut State,
) -> ReceiveResult<A> {
    ensure!(matches!(state.node, Node::NotActive));
    let sender = ctx.sender();
    ensure!(
        sender.matches_contract(&state.entry)
            || matches!(state.trusted, Some(trusted) if sender.matches_contract(&trusted))
    );
    let params: ActivateNodeParams = ctx.parameter_cursor().get()?;
    state.parent = params.parent;
    state.node = params.node;
    Ok(A::accept())
}

/// Insert new child into an internal node.
/// Called by a child node after they do a split.
#[receive(contract = "node", name = "insertBranch")]
fn contract_node_insert_branch<A: HasActions>(
    ctx: &impl HasReceiveContext,
    state: &mut State,
) -> ReceiveResult<A> {
    let node = match &mut state.node {
        Node::Internal(node) => node,
        _ => bail!(),
    };
    let sender = if let Address::Contract(address) = ctx.sender() {
        address
    } else {
        bail!()
    };
    ensure!(node.is_child_branch(&sender));

    let params: InsertBranchParams = ctx.parameter_cursor().get()?;

    if let InsertResult::Split = node.insert_branch(params.branch) {
        node.set_status(InternalNodeStatus::AwaitingNodes);
        Ok(A::send_raw(
            &state.entry,
            ReceiveName::new_unchecked("entry.requestNode"),
            Amount::from_micro_gtu(0),
            &[],
        ))
    } else {
        Ok(A::accept())
    }
}

// TODO: Unit testing
