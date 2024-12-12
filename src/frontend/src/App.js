import "./App.css";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import { faPaperclip, faArrowUp } from "@fortawesome/free-solid-svg-icons";
import { useState, useEffect } from "react";
import { db } from "./firebase"; // Firebase import
import {
  collection,
  addDoc,
  getDocs,
  query,
  orderBy,
} from "firebase/firestore"; // Firebase Firestore functions
import axios from "axios";
import Modal from "react-modal"; // Import the modal

Modal.setAppElement("#root"); // Set the root element for accessibility

function App() {
  const [message, setMessage] = useState("");
  const [messages, setMessages] = useState([]);
  const [isModalOpen, setIsModalOpen] = useState(false); // Modal state
  const [databaseType, setDatabaseType] = useState(null);
  const [selectedFile, setSelectedFile] = useState(null);
  const [pendingUpload, setPendingUpload] = useState(null);

  const formatAsTable = (jsonObject) => {
    if (!jsonObject || typeof jsonObject !== "object") return "Invalid data.";

    const { database_name, tables } = jsonObject;

    let formattedMessage = `Database: ${database_name}\n\n`;

    for (const [tableName, attributes] of Object.entries(tables)) {
      formattedMessage += `Table: ${tableName}\nAttributes:\n`;
      for (const [key, value] of Object.entries(attributes)) {
        formattedMessage += `${key}: ${value}\n`;
      }
      formattedMessage += "\n"; // Add a blank line between tables
    }

    return formattedMessage;
  };
  const cleanEscapedData = (data) => {
    if (typeof data === "string") {
      try {
        // Try parsing the string to remove backslashes if it's a valid JSON string
        return JSON.parse(data);
      } catch (error) {
        // If not parsable, return the original string
        return data;
      }
    } else if (Array.isArray(data)) {
      // If it's an array, recursively clean each element
      return data.map(cleanEscapedData);
    } else if (typeof data === "object" && data !== null) {
      // If it's an object, recursively clean each value
      return Object.fromEntries(
        Object.entries(data).map(([key, value]) => [
          key,
          cleanEscapedData(value),
        ])
      );
    } else {
      // For other data types (number, boolean, null), return as-is
      return data;
    }
  };

  // Fetch messages from Firestore when the component loads
  useEffect(() => {
    const fetchMessages = async () => {
      const q = query(collection(db, "messages"), orderBy("timestamp", "asc")); // Query to fetch messages in ascending order
      const querySnapshot = await getDocs(q);
      const loadedMessages = querySnapshot.docs.map((doc) => ({
        text: doc.data().text,
        fromSystem: doc.data().fromSystem || false, // default to false if not present
      }));
      setMessages(loadedMessages);
    };
    fetchMessages();
  }, []);

  // Function to handle the send button click
  const handleButtonClick = async () => {
    if (message.trim()) {
      const userMessage = message.trim();

      // Add the user's message to the chat
      setMessages((prevMessages) => [...prevMessages, { text: userMessage }]);

      try {
        // Send user input to the backend for processing
        const response = await axios.post(
          "http://localhost:5001/api/process_input",
          {
            user_input: userMessage,
          }
        );

        // Check the response status and handle accordingly
        if (response.status === 200) {
          const { message, captions, queries } = response.data;
          // Handle exit words (like "quit", "thanks")
          if (message.includes("Session ended")) {
            setMessages((prevMessages) => [
              ...prevMessages,
              { text: message, fromSystem: true },
            ]);
            setMessage(""); // Clear the input box before exiting
            return; // Exit early to avoid handling further logic
          }
          // Handle query-related responses
          if (queries && captions) {
            // Combine captions and queries for display
            const formattedResults = captions.map((caption, index) => {
              const query = queries[index] || ""; // Ensure no missing queries break the logic
              if (caption === "None\n") {
                // If caption is an empty string, only show the query
                return query;
              }
              return `${index + 1}. ${caption}${query}`; // Show both caption and query when caption is non-empty
            });

            // Join the formatted results with a line break
            const formattedResultsString = formattedResults.join("\n");

            setMessages((prevMessages) => [
              ...prevMessages,
              { text: message, fromSystem: true },
              { text: formattedResultsString, fromSystem: true },
            ]);
            setMessages((prevMessages) => [
              ...prevMessages,
              {
                text: "Let me know if you need further analysis or insights from this data. Type 'quit' to exit.",
                fromSystem: true,
              },
            ]);
          } else if (queries) {
            console.log("hi");
            // Handle cases with queries but no captions
            setMessages((prevMessages) => [
              ...prevMessages,
              { text: message, fromSystem: true },
              { text: queries, fromSystem: true },
            ]);
          } else {
            // Handle other generic success responses
            setMessages((prevMessages) => [
              ...prevMessages,
              { text: message, fromSystem: true },
            ]);
          }
        } else {
          // Handle unexpected backend responses
          setMessages((prevMessages) => [
            ...prevMessages,
            {
              text: "Could not process your input. Please try again.",
              fromSystem: true,
            },
          ]);
        }
      } catch (error) {
        console.error("Error processing input:", error);
        setMessages((prevMessages) => [
          ...prevMessages,
          {
            text: "An error occurred while processing your input.",
            fromSystem: true,
          },
        ]);
      }

      // Clear the input box after processing user input
      setMessage("");
    }
  };

  // Open the modal when the attach button is clicked
  const openModal = () => {
    setIsModalOpen(true);
  };

  // Close the modal
  const closeModal = () => {
    setIsModalOpen(false);
    setDatabaseType(null);
    setSelectedFile(null);
  };

  const handleDatabaseChoice = (type) => {
    setDatabaseType(type);
  };
  const handleFileChange = (event) => {
    setSelectedFile(event.target.files[0]);
  };

  const handlePrepareUpload = () => {
    if (selectedFile && databaseType) {
      setPendingUpload({ file: selectedFile, databaseType });
      setMessage(`Selected file: ${selectedFile.name}`);
      closeModal();
    } else {
      alert("Please select a file and database type.");
    }
  };

  const handleFileUpload = async () => {
    if (!pendingUpload) return;

    const formData = new FormData();
    formData.append("file", pendingUpload.file);
    formData.append("databaseType", pendingUpload.databaseType);

    try {
      const response = await fetch("http://localhost:5001/api/upload", {
        method: "POST",
        body: formData,
      });

      if (!response.ok) {
        const errorDetails = await response.text();
        console.error("File upload failed. Response:", errorDetails);
        throw new Error(errorDetails || "File upload failed");
      }

      const result = await response.json();
      const {
        result: databaseResult,
        sample_data: sampleData,
        prompt,
      } = result;
      const parsedSampleData = JSON.parse(sampleData);
      let cleanedData = cleanEscapedData(parsedSampleData);
      // Convert result.message to a string if it's an object
      const formattedMessage =
        typeof databaseResult === "object"
          ? formatAsTable(databaseResult) // Pass the object directly
          : databaseResult;
      alert(result.message);

      // Add the response message and processed data to the chat
      const uploadedFileMessage = `File uploaded: ${pendingUpload.file.name}`;
      setMessages((prevMessages) => [
        ...prevMessages,
        { text: uploadedFileMessage },
        { text: formattedMessage, fromSystem: true }, // Add formatted message to chat
        { text: "Here are some sample data:", fromSystem: true }, // Add introductory message
        { text: JSON.stringify(cleanedData, null, 2), fromSystem: true },
        { text: prompt, fromSystem: true },
      ]);
      await addDoc(collection(db, "messages"), {
        text: uploadedFileMessage,
        timestamp: new Date(),
      });
      await addDoc(collection(db, "messages"), {
        text: formattedMessage,
        timestamp: new Date(),
        fromSystem: true,
      });
      await addDoc(collection(db, "messages"), {
        text: result.prompt,
        timestamp: new Date(),
        fromSystem: true,
      });
      setPendingUpload(null); // Reset pending upload after completion
      setMessage(""); // Clear the input box
      setDatabaseType(null);
      setSelectedFile(null);
    } catch (error) {
      console.error("Error uploading file:", error);
      setPendingUpload(null); // Reset pending upload after completion
      setMessage(""); // Clear the input box
      setDatabaseType(null);
      setSelectedFile(null);
      alert("File upload failed.");
    }
  };

  return (
    <div className="App">
      <header className="App-header" style={{ marginLeft: 10 }}>
        <h3>Welcome to ChatDB!</h3>
        <hr style={{ width: "100%", borderColor: "#ccc" }} />
      </header>

      {/* Chat container to display messages */}
      <div className="chat-container">
        {messages.map((msg, index) => {
          // Ensure each message is valid before rendering
          if (typeof msg.text !== "string") {
            console.error("Invalid message detected:", msg);
            return null; // Skip invalid messages
          }

          return (
            <div
              key={index}
              className={`chatbox ${
                msg.fromSystem ? "system-message" : "my-message"
              }`}
            >
              {msg.text}
            </div>
          );
        })}
      </div>

      {/* Input box with attachment and send button */}
      <div className="input-container">
        <button className="attach-button" onClick={openModal}>
          <FontAwesomeIcon icon={faPaperclip} size="lg" />
        </button>
        <input
          type="text"
          placeholder="Type your message(remember to type in 'quit' when you want to upload a new database)"
          className="input-box"
          value={message}
          onChange={(e) => setMessage(e.target.value)} // Update the message as you type
        />
        <button
          className="scroll-button"
          onClick={pendingUpload ? handleFileUpload : handleButtonClick}
        >
          <FontAwesomeIcon icon={faArrowUp} size="lg" />
        </button>
      </div>

      {/* Modal for SQL/NoSQL choice */}
      <Modal
        isOpen={isModalOpen}
        onRequestClose={closeModal}
        contentLabel="Database Type Choice"
        className="Modal"
        overlayClassName="Overlay"
      >
        <h2>Select Database Type</h2>
        <button onClick={() => handleDatabaseChoice("SQL")}>SQL</button>
        <button onClick={() => handleDatabaseChoice("NoSQL")}>NoSQL</button>
        <button onClick={closeModal}>Cancel</button>

        {databaseType && (
          <div style={{ marginTop: "20px" }}>
            <h3>Upload {databaseType} File</h3>
            <input type="file" onChange={handleFileChange} />
            <button onClick={handlePrepareUpload} style={{ marginTop: 10 }}>
              Add to Chat
            </button>
          </div>
        )}
      </Modal>
    </div>
  );
}

export default App;
