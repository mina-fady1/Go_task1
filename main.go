package main

import (
	"bufio"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
)

// Message represents a chat message or system notification.
type Message struct {
	Sender string
	Text   string
	Type   string // "join", "leave", "msg"
}

// Client represents a connected user running in its own goroutine.
type Client struct {
	Name        string
	DisplayPipe chan Message
	quit        chan struct{}
	closeOnce   sync.Once
}

// Close safely shuts down the client's goroutine.
func (c *Client) Close() {
	c.closeOnce.Do(func() {
		close(c.quit)
	})
}

// ChatServer manages client connections and routes messages using channels.
type ChatServer struct {
	JoinPipe    chan *Client // Pipe 1: User joins
	MessagePipe chan Message // Pipe 2: Chat messages
	LeavePipe   chan string  // Pipe 3: User departures
	shutdown    chan struct{}

	clients map[string]*Client
	mu      sync.Mutex
	wg      sync.WaitGroup
	stopped sync.Once
}

// NewServer initializes the ChatServer with buffered channels.
func NewServer() *ChatServer {
	return &ChatServer{
		JoinPipe:    make(chan *Client, 10),
		MessagePipe: make(chan Message, 100),
		LeavePipe:   make(chan string, 10),
		shutdown:    make(chan struct{}),
		clients:     make(map[string]*Client),
	}
}

// Run starts the central server event loop using select.
func (s *ChatServer) Run() {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			select {
			// 1. JOIN PIPE: Handle new user joining
			case client := <-s.JoinPipe:
				s.mu.Lock()
				s.clients[client.Name] = client
				s.broadcastUnlocked(Message{
					Sender: client.Name,
					Type:   "join",
					Text:   fmt.Sprintf("User %s joined the chat.", client.Name),
				})
				s.mu.Unlock()

			// 2. LEAVE PIPE: Handle user departure / removal
			case name := <-s.LeavePipe:
				s.mu.Lock()
				if client, exists := s.clients[name]; exists {
					client.Close()
					delete(s.clients, name)

					s.broadcastUnlocked(Message{
						Sender: name,
						Type:   "leave",
						Text:   fmt.Sprintf("User %s left the chat.", name),
					})
				}
				s.mu.Unlock()

			// 3. MESSAGE PIPE: Broadcast chat messages
			case msg := <-s.MessagePipe:
				s.mu.Lock()
				s.broadcastUnlocked(msg)
				s.mu.Unlock()

			// 4. SHUTDOWN: Disconnect all users cleanly
			case <-s.shutdown:
				s.mu.Lock()
				for name, client := range s.clients {
					client.Close()
					delete(s.clients, name)
				}
				s.mu.Unlock()
				return
			}
		}
	}()
}

// broadcastUnlocked sends a message to all connected clients except the sender.
func (s *ChatServer) broadcastUnlocked(msg Message) {
	for _, client := range s.clients {
		if client.Name != msg.Sender {
			select {
			case client.DisplayPipe <- msg:
			default:
			}
		}
	}
}

// runClient runs a dedicated goroutine for each connected user (Display Pipe).
func (s *ChatServer) runClient(c *Client) {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			select {
			case msg, ok := <-c.DisplayPipe:
				if !ok {
					return
				}
				if msg.Type == "join" || msg.Type == "leave" {
					fmt.Printf("[Notification for %s] %s\n", c.Name, msg.Text)
				} else {
					fmt.Printf("[Incoming for %s] %s\n", c.Name, msg.Text)
				}
			case <-c.quit:
				return
			}
		}
	}()
}

// Stop initiates clean server shutdown and waits for all goroutines to finish.
func (s *ChatServer) Stop() {
	s.stopped.Do(func() {
		close(s.shutdown)
	})
	s.wg.Wait()
	fmt.Println("\nServer shut down cleanly. All goroutines stopped.")
}

func main() {
	server := NewServer()
	server.Run()

	// Handle Ctrl+C (SIGINT) and SIGTERM for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	done := make(chan struct{})
	go func() {
		select {
		case <-sigChan:
			server.Stop()
			os.Exit(0)
		case <-done:
		}
	}()

	reader := bufio.NewReader(os.Stdin)
	var activeUser string

	printMenu()

	for {
		if activeUser == "" {
			fmt.Print("> ")
		} else {
			fmt.Printf("[%s] > ", activeUser)
		}

		input, err := reader.ReadString('\n')
		if err != nil {
			break
		}
		input = strings.TrimSpace(input)
		if input == "" {
			continue
		}

		parts := strings.SplitN(input, " ", 2)
		command := strings.ToLower(parts[0])
		arg := ""
		if len(parts) > 1 {
			arg = parts[1]
		}

		switch command {
		case "/create", "/join", "1":
			if arg == "" {
				fmt.Print("Enter username: ")
				if line, err := reader.ReadString('\n'); err == nil {
					arg = strings.TrimSpace(line)
				}
			}
			if arg == "" {
				fmt.Println("Error: Username cannot be empty.")
				continue
			}

			server.mu.Lock()
			_, exists := server.clients[arg]
			if exists {
				server.mu.Unlock()
				fmt.Println("Error: Username already taken.")
			} else {
				client := &Client{
					Name:        arg,
					DisplayPipe: make(chan Message, 100),
					quit:        make(chan struct{}),
				}
				server.runClient(client)
				server.JoinPipe <- client
				server.mu.Unlock()

				fmt.Printf("User '%s' created and joined.\n", arg)
				if activeUser == "" {
					activeUser = arg
					fmt.Printf("Auto-selected active user: %s\n", activeUser)
				}
			}

		case "/list", "/users", "2":
			server.mu.Lock()
			fmt.Println("--- Connected Users ---")
			if len(server.clients) == 0 {
				fmt.Println("(none)")
			} else {
				for name := range server.clients {
					if name == activeUser {
						fmt.Printf("- %s (active)\n", name)
					} else {
						fmt.Printf("- %s\n", name)
					}
				}
			}
			server.mu.Unlock()

		case "/select", "/switch", "3":
			if arg == "" {
				fmt.Print("Enter username to act as: ")
				if line, err := reader.ReadString('\n'); err == nil {
					arg = strings.TrimSpace(line)
				}
			}
			if arg == "" {
				fmt.Println("Error: Username cannot be empty.")
				continue
			}

			server.mu.Lock()
			_, exists := server.clients[arg]
			server.mu.Unlock()

			if !exists {
				fmt.Println("Error: User does not exist.")
			} else {
				activeUser = arg
				fmt.Printf("Active user changed to: %s\n", activeUser)
			}

		case "/send", "/msg", "4":
			if activeUser == "" {
				fmt.Println("Error: No active user selected. Use /select <username> first.")
				continue
			}
			if arg == "" {
				fmt.Print("Enter message text: ")
				if line, err := reader.ReadString('\n'); err == nil {
					arg = strings.TrimSpace(line)
				}
			}
			if arg == "" {
				fmt.Println("Error: Message cannot be empty.")
				continue
			}

			server.MessagePipe <- Message{
				Sender: activeUser,
				Type:   "msg",
				Text:   fmt.Sprintf("[%s]: %s", activeUser, arg),
			}
			fmt.Println("Message sent.")

		case "/remove", "/leave", "5":
			if arg == "" {
				fmt.Print("Enter username to remove: ")
				if line, err := reader.ReadString('\n'); err == nil {
					arg = strings.TrimSpace(line)
				}
			}
			if arg == "" {
				fmt.Println("Error: Username cannot be empty.")
				continue
			}

			server.mu.Lock()
			_, exists := server.clients[arg]
			server.mu.Unlock()

			if !exists {
				fmt.Println("Error: User does not exist.")
			} else {
				server.LeavePipe <- arg
				fmt.Printf("User '%s' removed.\n", arg)
				if activeUser == arg {
					activeUser = ""
					fmt.Println("Active user was removed. Please /select another user.")
				}
			}

		case "/help", "help", "menu", "6":
			printMenu()

		case "/exit", "7", "exit", "quit":
			close(done)
			server.Stop()
			return

		default:
			// If a user is active and typed text without a slash command, send it directly as a message!
			if activeUser != "" {
				server.MessagePipe <- Message{
					Sender: activeUser,
					Type:   "msg",
					Text:   fmt.Sprintf("[%s]: %s", activeUser, input),
				}
				fmt.Println("Message sent.")
			} else {
				fmt.Println("Unknown command. Type /help for a list of commands.")
			}
		}
	}
}

func printMenu() {
	fmt.Println("=========================================")
	fmt.Println("   Concurrent Chat System (Terminal UI)  ")
	fmt.Println("=========================================")
	fmt.Println(" Commands:")
	fmt.Println(" 1 /create <name>   - Create a new connected user")
	fmt.Println(" 2 /list            - List all connected users")
	fmt.Println(" 3 /select <name>   - Select which user to act as")
	fmt.Println(" 4 /send <msg>      - Send a message as the selected user")
	fmt.Println(" 5 /remove <name>   - Remove a user from the server")
	fmt.Println(" 6 /help            - Show this menu again")
	fmt.Println(" 7 /exit            - Shut down and exit")
	fmt.Println("=========================================")
}
