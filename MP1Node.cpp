/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/
#include <random>
#include "MP1Node.h"

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */
class MemberCompareLessThan {
public:
		bool operator() (MemberListEntry  entry1, MemberListEntry entry2) const {
			return ( (entry1.getid() < entry2.getid())  || ((entry1.getid() == entry2.getid()) && (entry1.getport() < entry2.getport())) );
		}
};

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address) {
	for( int i = 0; i < 6; i++ ) {
		NULLADDR[i] = 0;
	}
	this->memberNode = member;
	this->emulNet = emul;
	this->log = log;
	this->par = params;
	this->memberNode->addr = *address;
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into the queue
 * 				This function is called by a node to receive messages currently waiting for it
 */
int MP1Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1, &(memberNode->mp1q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: nodeStart
 *
 * DESCRIPTION: This function bootstraps the node
 * 				All initializations routines for a member.
 * 				Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport) {
    Address joinaddr;
    joinaddr = getJoinAddress();

    // Self booting routines
    if( initThisNode(&joinaddr) == -1 ) {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
        exit(1);
    }

    if( !introduceSelfToGroup(&joinaddr) ) {
        finishUpThisNode();
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
#endif
        exit(1);
    }

    return;
}

/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */
int MP1Node::initThisNode(Address *joinaddr) {
	/*
	 * This function is partially implemented and may require changes
	 */
	int id = *(int*)(&memberNode->addr.addr);
	int port = *(short*)(&memberNode->addr.addr[4]);

	memberNode->bFailed = false;
	memberNode->inited = true;
	memberNode->inGroup = false;
    // node is up!
	memberNode->nnb = 0;
	memberNode->heartbeat = 0;
	memberNode->pingCounter = TFAIL;
	memberNode->timeOutCounter = -1;
    initMemberListTable(memberNode);

    return 0;
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
	MessageHdr *msg;
#ifdef DEBUGLOG
    static char s[1024];
#endif

    if ( 0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr), sizeof(memberNode->addr.addr))) {
        // I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Starting up group...");
#endif
        //add myself to the memberlist
        updateMyPos();

        memberNode->inGroup = true;
    }
    else {
        size_t msgsize = sizeof(MessageHdr) + sizeof(joinaddr->addr) + sizeof(long) + 1;
        msg = (MessageHdr *) malloc(msgsize * sizeof(char));

        // create JOINREQ message: format of data is {struct Address myaddr}
        msg->msgType = JOINREQ;
        memcpy((char *)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
        memcpy((char *)(msg+1) + 1 + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));

#ifdef DEBUGLOG
        sprintf(s, "Trying to join...");
        log->LOG(&memberNode->addr, s);
#endif

        // send JOINREQ message to introducer member
        emulNet->ENsend(&memberNode->addr, joinaddr, (char *)msg, msgsize);

        free(msg);
    }

    return 1;
}

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode(){
   /*
    * Your code goes here
    */
}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 * 				Check your messages in queue and perform membership protocol duties
 */
void MP1Node::nodeLoop() {
    if (memberNode->bFailed) {
    	return;
    }

    // Check my messages
    vector<MemberListEntry> newNodes;

    checkMessages(newNodes);

    // Wait until you're in the group...
    if( !memberNode->inGroup ) {
    	return;
    }

    // ...then jump in and share your responsibilites!
    nodeLoopOps(newNodes);

    return;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message handler
 */
void MP1Node::checkMessages(vector<MemberListEntry> &newNodes) {
    void *ptr;
    int size;

    // Pop waiting messages from memberNode's mp1q
    while ( !memberNode->mp1q.empty() ) {
    	ptr = memberNode->mp1q.front().elt;
    	size = memberNode->mp1q.front().size;
    	memberNode->mp1q.pop();
    	recvCallBack((void *)memberNode, (char *)ptr, size, newNodes);
    }
    return;
}
/**
 * FUNCTION NAME: joinrepCallBack
 *
 * DESCRIPTION: process a JOINREQ request
 */
void MP1Node::joinreqCallBack(char *data, int size, vector<MemberListEntry> &newNodes ) {

	MessageHdr *msg = (MessageHdr *) data;
	char addr[6];
	long heartbeat;

	memcpy(addr, (char *)(msg+1), sizeof(memberNode->addr.addr));
	memcpy(&heartbeat, (char *)(msg+1) + 1 + sizeof(memberNode->addr.addr), sizeof(long));

	int id = *(int*)(&addr[0]);
	int port = *(short*)(&addr[4]);

	if (updatelistCallBack(data, size)) {
		//add the new entry in the list of new nodes
		MemberListEntry new_entry(id, port, heartbeat, par->getcurrtime());
		newNodes.push_back(new_entry);
	}


	//send JOINREP msg to the new entry
	MessageHdr rep;
	rep.msgType = JOINREP;
	Address toaddr(to_string(id) + ":" + to_string(port));

	emulNet->ENsend(&memberNode->addr, &toaddr, (char *) &rep, sizeof(MessageHdr));
}

/**
 * FUNCTION NAME: updatelistCallBack
 *
 * DESCRIPTION: update an entry in the membership list. Return true if a new entry was added in the
 * membership list otherwise return false
 */
bool MP1Node::updatelistCallBack(char *data, int size) {
	MessageHdr *msg = (MessageHdr *) data;
	char addr[6];
	long heartbeat;

	memcpy(addr, (char *)(msg+1), sizeof(memberNode->addr.addr));
	memcpy(&heartbeat, (char *)(msg+1) + 1 + sizeof(memberNode->addr.addr), sizeof(long));

	int id = *(int*)(&addr[0]);
	int port = *(short*)(&addr[4]);

	MemberListEntry new_entry(id, port, heartbeat, par->getcurrtime());

	vector<MemberListEntry>::iterator it = lower_bound(memberNode->memberList.begin(), memberNode->memberList.end(), new_entry, MemberCompareLessThan());

#ifdef DEBUGLOG
    static char s[1024];
#endif

	if (it != memberNode->memberList.end() && it->getid() == id && it->getport() == port) {
		//the node is already in the list, just update it
		if (it->getheartbeat() < heartbeat) {
			*it = new_entry;
#ifdef DEBUGLOG
			//sprintf(s, "Updating heartbeat of %d:%d to %ld in membership list", id, port, heartbeat);
			//log->LOG(&memberNode->addr, s);
#endif
		}

		return false;
	}
	else {
		//add the new node to the membership list
		memberNode->memberList.push_back(new_entry);

		Address newaddress(to_string(id) + ":" + to_string(port));
		log->logNodeAdd(&memberNode->addr, &newaddress);

		sort(memberNode->memberList.begin(), memberNode->memberList.end(), MemberCompareLessThan());

		return true;
	}
}

/**
 * FUNCTION NAME: updateMyPos
 *
 * DESCRIPTION: update memberNode->myPos
 */
void MP1Node::updateMyPos() {
	int myid = *(int*)(&memberNode->addr.addr[0]);
	int myport = *(short*)(&memberNode->addr.addr[4]);

	MemberListEntry my_entry(myid, myport, memberNode->heartbeat, par->getcurrtime());

	memberNode->myPos = lower_bound(memberNode->memberList.begin(), memberNode->memberList.end(), my_entry, MemberCompareLessThan());

	if (memberNode->myPos == memberNode->memberList.end() || (memberNode->myPos->getid() != myid && memberNode->myPos->getport() != myport)) {
		//this node's entry is not in the memberlist! add it
		memberNode->memberList.push_back(my_entry);
		sort(memberNode->memberList.begin(), memberNode->memberList.end(), MemberCompareLessThan());
		memberNode->myPos = lower_bound(memberNode->memberList.begin(), memberNode->memberList.end(), my_entry, MemberCompareLessThan());
	}
}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size, vector<MemberListEntry> &newNodes ) {
	/*
	 * Your code goes here
	 */
	Member *mNode = (Member *) env;
	MessageHdr *msg = (MessageHdr *) data;

	switch(msg->msgType) {
	case JOINREQ:
		//this is a join request from a new node
		joinreqCallBack(data, size, newNodes);
		break;
	case JOINREP:
		//this is a join reply from the introducer
		memberNode->inGroup = true;
		break;
	case LIST:
		//update membership list
		updatelistCallBack(data, size);
		break;
	default:
		break;
	}
	return true;
}

/**
 * FUNCTION NAME: sendMemberList
 *
 * DESCRIPTION: send current member list to a node with a given address
 */
void MP1Node::sendMemberList(Address *toaddr, int localtime) {
	if (!toaddr) return;

	MessageHdr *msg;
	size_t msgsize = sizeof(MessageHdr) + sizeof(toaddr->addr) + sizeof(long) + 1;
	msg = (MessageHdr *) malloc(msgsize * sizeof(char));
	msg->msgType = LIST;

#ifdef DEBUGLOG
    static char s[1024];
#endif

	for(vector<MemberListEntry>::iterator it = memberNode->memberList.begin();
	    it != memberNode->memberList.end();
	    ++it) {
		//skip failed member but not removed yet
		if (localtime - it->gettimestamp() >= TFAIL) {
			continue;
		}
        // create LIST message: format of data is (addr, heartbeat)
		Address entry_addr(to_string(it->getid()) + ":" + to_string(it->getport()));

        memcpy((char *)(msg+1), &(entry_addr.addr), sizeof(toaddr->addr));
        memcpy((char *)(msg+1) + 1 + sizeof(toaddr->addr), &(it->heartbeat), sizeof(long));

#ifdef DEBUGLOG
        //sprintf(s, "Send heartbeat (%ld) of %s to %s", it->heartbeat, entry_addr.getAddress().c_str(), toaddr->getAddress().c_str());
        //log->LOG(&memberNode->addr, s);
#endif

        // send memberlist message to one node
        emulNet->ENsend(&memberNode->addr, toaddr, (char *)msg, msgsize);
	}

	free(msg);
}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps(vector<MemberListEntry> &newNodes) {

	int localtime = par->getcurrtime();

	//update memberNode->myPos
	updateMyPos();

	//increase my heartbeat and update myself
	memberNode->heartbeat++;

	memberNode->myPos->setheartbeat(memberNode->heartbeat++);
	memberNode->myPos->settimestamp(localtime);

	MemberListEntry my_entry = *(memberNode->myPos);

	vector<MemberListEntry> & memberlist = memberNode->memberList;
	int len = memberlist.size();

#ifdef DEBUGLOG
    static char s[1024];
#endif

    int numfailed = 0;

	//remove nodes that hasn't responded within TREMOVE
	for(int i=len-1; i>=0; --i) {
		long difftime = localtime - memberlist[i].gettimestamp();
		if (difftime >= TFAIL) {
			numfailed++;
			if ( difftime >= TREMOVE) {
				int currlen = memberlist.size();

				Address toremove(to_string(memberlist[i].getid()) + ":" + to_string(memberlist[i].getport()));
				log->logNodeRemove(&memberNode->addr, &toremove);

				swap(memberlist[i], memberlist[currlen-1]);
				memberlist.pop_back();

			}
		}
	}

	sort(memberlist.begin(), memberlist.end(), MemberCompareLessThan());
	memberNode->myPos = lower_bound(memberlist.begin(), memberlist.end(), my_entry, MemberCompareLessThan());

	//get list of nodes to gossip with
	random_device rd;
	mt19937 mt(rd());
	uniform_int_distribution<> dist(0, memberlist.size()-1);

	vector<MemberListEntry> gossipnodes(newNodes);

	int maxneighbors = 5;
	int n = gossipnodes.size();

	int myid = memberNode->myPos->getid();
	int myport = memberNode->myPos->getport();

	//cout << myid << ":" << myport << " get nodes to gossip with #members = " << memberlist.size() << " n = " << n << endl;
	int numpotentialneighbors = memberlist.size()-1-numfailed;
	bool skipfailed = (numpotentialneighbors > 0) ? true : false;

	while (n < maxneighbors && n < numpotentialneighbors) {
		int ix = dist(mt);
		//cout << "ix = " << ix << "(" << memberlist[ix].getid() << ":" << memberlist[ix].getport() << ")" << endl;

		if (memberlist[ix].getid() != myid || memberlist[ix].getport() != myport) {
			if (skipfailed && (localtime - memberlist[ix].gettimestamp() >= TFAIL) ){
				//do not send membership list to failed nodes
				continue;
			}
			bool found = false;
			for(int i=0; i<gossipnodes.size(); ++i) {
				if (gossipnodes[i].getid() == memberlist[ix].getid() && gossipnodes[i].getport() == memberlist[ix].getport()) {
					//cout << ix << " is already in gossipnodes" << endl;
					found = true;
					break;
				}
			}
			if (!found) {
				//cout << "added " << ix << " to gossipnodes" << endl;
				gossipnodes.push_back(memberlist[ix]);
				++n;
			}
		}
	}

	for(auto member : gossipnodes) {
		Address addr(to_string(member.getid()) + ":" + to_string(member.getport()));
		sendMemberList(&addr, localtime);
	}
}

/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr) {
	return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
}

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getJoinAddress() {
    Address joinaddr;

    memset(&joinaddr, 0, sizeof(Address));
    *(int *)(&joinaddr.addr) = 1;
    *(short *)(&joinaddr.addr[4]) = 0;

    return joinaddr;
}

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */
void MP1Node::initMemberListTable(Member *memberNode) {
	memberNode->memberList.clear();
}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr)
{
    printf("%d.%d.%d.%d:%d \n",  addr->addr[0],addr->addr[1],addr->addr[2],
                                                       addr->addr[3], *(short*)&addr->addr[4]) ;    
}
