Name	: Vipul Chaskar
Email	: vchaska1@binghamton.edu
BNumber	: B00716752

CS557 - Programming Assignment 3
The Snapshot Algorithm

Programming Language: Python 2.7

————————————————————
How to run the code:
————————————————————
* The files which need to be run for branch and controller are “branch.py” and “controller.py”


* Make sure the following things:
	-> You're in BASH shell and have the path "/home/phao3/protobuf/bin/bin" appended to $PATH environment variable.
	-> You have generated the message definition file from bank.proto (bank_pb2.py) in the same directory (Included with the submission).
	-> The source code files have executable permission:
		$ chmod +x *.py


* Run the Branches by typing the following command:
	$ ./branch.py <branch_name> <port_no>
E.g.	$ ./branch.py branch1 9990


* Add information about these branches in the “branches.txt”.


* Run the controller:
	$ ./controller.py <total_amount> <branches.txt>
E.g.	$ ./controller.py 5000 branches.txt


* Alternatively, you can also call these scripts with Python interpreter:
	$ python branch.py branch1 9990
	$ python controller.py 5000 branches.txt


——————————————————————————————
Description of Implementation:
——————————————————————————————

Each branch is a multithreaded process. The main thread continuously listens for incoming connections and creates an instance of ClientThread object to handle each incoming connection.

On receiving the init_branch message, a branch performs following things:
* From the list of branches given, connect to branches whose name is lexicographically smaller than own. Similarly, the branches with lexicographically greater name will connect to this branch. This ensures that there is only one connection between any pair of branches.
* Record the given balance in BankVault
* Start a thread which will periodically withdraw random amount of money from BankVault and send it to a randomly selected branch (MoneyTransferThread).
* Keep listening for further messages from controller (such as snapshot)

On connecting to or accepting connection from another branch:
* Create a thread to handle connection with this remote branch.
* Add this thread to locally maintained ThreadPool.
* Begin receiving and processing the events from this branch.

On receiving an init_snapshot message:
* Temporarily pause the MoneyTransferThread.
* Record the local state.
* Send marker message on all other channels and start recording incoming activity.
* Resume the MoneyTransferThread.

On receiving marker message:
* Temporarily pause the MoneyTransferThread.
* If it is first marker message,
	* Record local state
	* Mark the state of incoming channel from sender to itself as empty
	* Send marker message to all outgoing channels
	* Start recording incoming activity on all channels. This is accomplished by 
	  adding a recorder (essentially a counter initialised to 0, uniquely identified
	  by snapshot_id). So whenever a branch receives certain amount on a channel, the
	  counters of currently present recorders (if any) is incremented by the received amount.
* Otherwise,
	* Get the state of the channel on which this marker was received and record it.
	  This is accomplished by popping the recorder identified by current snapshot_id
	  and returning its amount.
* Resume the MoneyTransferThread.

On receiving retrieve snapshot message:
* Check if the snapshot with the given snapshot_id is captured. Error out if it is not.
* Create the return_snapshot object and populate it with the local and channel states associated with the given snapshot_id.
* Send the return_snapshot message to controller.


———————————————————
Notes:
———————————————————

* A branch prints out on the console the following things:
	* Own name and the port it is listening on
	* Name of the remote branch when it connects to one
	* Initial balance when it is initialised
	* Money sent/received to/from a remote branch, along with updated balance
	* On receiving and sending a marker/init_snapshot message
	  (This makes it easy to verify how much balance is expected to be captured in
	  local state when snapshot is being taken)


* The controller sends an init_snapshot event to a random branch every “10 seconds” and this snapshot is retrieved after “5 seconds”. These values are customisable and defined at the top of controller.py.


———————————————————
Sample outputs:
———————————————————

Branch:-

vchaska1@remote06:~/proj3$ ./branch.py branch1 9990
branch1 listening on remote06:9990
Press Ctrl+C to terminate...
Connected to : branch2
Connected to : branch3
Connected to : branch5
Connected to : branch4
Branch initialized with initial balance : 1000
Sending 20 to branch3. New balance is : 980
Received 50 from branch5. New balance is : 1030
Received 10 from branch3. New balance is : 1040
Sending 41 to branch5. New balance is : 999
Got the first marker msg from branch2 (snapshot_id: 1)
Sending marker message to : branch2 (snapshot_id: 1)
Sending marker message to : branch3 (snapshot_id: 1)
Sending marker message to : branch5 (snapshot_id: 1)
Sending marker message to : branch4 (snapshot_id: 1)
Got reply marker msg from: branch3 (snapshot_id: 1)
Got reply marker msg from: branch4 (snapshot_id: 1)
Got reply marker msg from: branch5 (snapshot_id: 1)
Received 40 from branch4. New balance is : 1039
Received 52 from branch3. New balance is : 1091
Received 19 from branch2. New balance is : 1110
Sending 22 to branch2. New balance is : 1088


Controller:-

vchaska1@remote06:~/proj3$ ./controller.py 5000 branches.txt
Sending 1000 per branch to these branches: ['branch1', 'branch2', 'branch3', 'branch4', 'branch5']

Sent snapshot msg 1 to branch2

snapshot_id: 1
branch1: 999, branch2->branch1: 0, branch3->branch1: 0, branch4->branch1: 0, branch5->branch1: 0,
branch2: 970, branch1->branch2: 0, branch3->branch2: 0, branch4->branch2: 0, branch5->branch2: 0,
branch3: 1040, branch1->branch3: 0, branch2->branch3: 0, branch4->branch3: 0, branch5->branch3: 0,
branch4: 1009, branch1->branch4: 0, branch2->branch4: 0, branch3->branch4: 0, branch5->branch4: 0,
branch5: 982, branch1->branch5: 0, branch2->branch5: 0, branch3->branch5: 0, branch4->branch5: 0,
