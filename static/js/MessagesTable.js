const { useState, useEffect } = React;

function MessagesTable() {
    const [messages, setMessages] = useState([]);
    const [selectedMessages, setSelectedMessages] = useState(new Set());
    const [selectedDate, setSelectedDate] = useState("");
    const [selectedOption, setSelectedOption] = useState("");

    // Fetch messages from Flask
    const loadMessages = (event) => {
        event.preventDefault(); // Prevent form submission
        console.log("Loading messages for date:", selectedDate, "and option:", selectedOption);

        const queryParams = new URLSearchParams({
            date: selectedDate,
            option: selectedOption
        }).toString();

        fetch(`/loadmessages?${queryParams}`)
            .then(response => response.json())
            .then(data => {
                setMessages(data);
                setSelectedMessages(new Set()); // Reset selection when loading new messages
            })
            .catch(error => console.error("Error fetching messages:", error));
    };

    // Handle message selection
    const toggleSelection = (msgId) => {
        console.log("Toggling selection for message ID:", msgId);
        setSelectedMessages((prevSelected) => {
            const newSelected = new Set(prevSelected);
            if (newSelected.has(msgId)) {
                newSelected.delete(msgId);
            } else {
                newSelected.add(msgId);
            }
            return newSelected;
        });

    };

    useEffect(() => {
        console.log("Updated selected messages:", Array.from(selectedMessages).join(", "));
    }, [selectedMessages]);

    // Push selected messages to Flask
    const pushMessages = (event) => {
        event.preventDefault(); // Prevent form submission
        console.log("Pushing selected messages:", Array.from(selectedMessages).join(", "));
        const selectedData = messages.filter(msg => selectedMessages.has(msg.id));

        if (selectedData.length === 0) {
            alert("No messages selected!");
            return;
        }

        fetch("/pushmessage", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ messages: selectedData })
        })
            .then(response => response.json())
            .then(data => {
                console.log("Messages pushed successfully: " + JSON.stringify(data));
                loadMessages(event);
            })// ✅ Reload table after push)
            .catch(error => console.error("Error pushing messages:", error));
    };

    

    return (
        <div>
            <form>
                <label>
                    Date:
                    <input
                        type="date"
                        value={selectedDate}
                        onChange={(e) => setSelectedDate(e.target.value)}
                        required
                    />
                </label>
                <br /><br />

                <label>
                    Option:
                    <select
                        value={selectedOption}
                        onChange={(e) => setSelectedOption(e.target.value)}
                        required
                    >
                        <option value="">-- Choose One --</option>
                        <option value="latest">latest</option>
                        <option value="earliest">earliest</option>
                    </select>
                </label>
                <br /><br />

                <button onClick={loadMessages}>Load Messages</button>

            </form>
            
            <br /><br />
                <button onClick={pushMessages}>Push Selected Messages</button>

            <br />
            <h2>Kafka Messages</h2>
            <table border="1">
                <thead>
                    <tr>
                        <th>Select</th>
                        <th>Update Type</th>
                        <th>Date</th>
                        <th>SN</th>
                        <th>JSON</th>
                    </tr>
                </thead>
                <tbody>
                    {messages.map((msg) => (
                        <tr key={msg.id}>
                            <td>
                                <input
                                    type="checkbox"
                                    checked={selectedMessages.has(msg.id)}
                                    onChange={() => toggleSelection(msg.id)}
                                />
                            </td>
                            <td>{msg.timestamp_type}</td>
                            <td>{msg.timestamp_value}</td>
                            <td>{msg.id}</td>
                            <td>{msg.message_json}</td>
                        </tr>
                    ))}
                </tbody>
            </table>


        </div>
    );
}

// Render the component inside the root div
ReactDOM.createRoot(document.getElementById("root")).render(<MessagesTable />);
