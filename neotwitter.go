package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/VividCortex/robustly"
	"github.com/mrjones/oauth"
	"github.com/wfreeman/GonormCypher"
)

func Usage() {
	fmt.Println("Usage:")
	fmt.Print("go run neotwitter.go")
	fmt.Print("  --consumerkey <consumerkey>")
	fmt.Println("  --consumersecret <consumersecret>")
}

var g *gonorm.Gonorm

var consumerKey *string = flag.String(
	"consumerkey",
	"",
	"Consumer Key from Twitter. See: https://dev.twitter.com/apps/new")

var consumerSecret *string = flag.String(
	"consumersecret",
	"",
	"Consumer Secret from Twitter. See: https://dev.twitter.com/apps/new")
var accessTokenKey *string = flag.String(
	"accesstoken",
	"",
	"Access Token from Twitter.")

var accessTokenSecret *string = flag.String(
	"accesstokensecret",
	"",
	"Access Token Secret from Twitter.")

func init() {
	g = gonorm.New("http://localhost", 7474)
}

func main() {
	flag.Parse()

	if len(*consumerKey) == 0 || len(*consumerSecret) == 0 {
		fmt.Println("You must set the --consumerkey and --consumersecret flags.")
		fmt.Println("---")
		Usage()
		os.Exit(1)
	}
	// add go before this to make both run
	go robustly.Run(func() { loop() })
	robustly.Run(func() { hydrateUsers() })
}

func loop() {
	c := oauth.NewConsumer(
		*consumerKey,
		*consumerSecret,
		oauth.ServiceProvider{
			RequestTokenUrl:   "http://api.twitter.com/oauth/request_token",
			AuthorizeTokenUrl: "https://api.twitter.com/oauth/authorize",
			AccessTokenUrl:    "https://api.twitter.com/oauth/access_token",
		})

	accessToken := &oauth.AccessToken{*accessTokenKey, *accessTokenSecret}
	for {
		user := getNextUser()
		friends := getFriends(user, c, accessToken)
		connectFriends(user, friends)

		fmt.Println("sleeping for 30 seconds")
		time.Sleep(30 * time.Second)

	}
}

func connectFriends(user User, friends []User) {
	for _, friend := range friends {
		fmt.Printf("connecting %d to %d\n", user.Id, friend.Id)
		g.Cypher(`
      MERGE (u:User {id:{id}})
      MERGE (friend:User {id:{friendId}})
      MERGE (u)-[:FOLLOWS]->(friend)
      `).On(map[string]interface{}{
			"id":       user.Id,
			"friendId": friend.Id,
		}).Execute()
		//fmt.Println(response)
	}
}

type User struct {
	Id          uint64 `json:"id"`
	ScreenName  string `json:"screen_name"`
	Description string `json:"description"`
	Name        string `json:"name"`
}

func getNextUser() User {
	result := g.Cypher(`
     MATCH p=(u:User)-[:FOLLOWS*2]->(n)
     WHERE u.id = 221902776
     AND NOT (n)-[:FOLLOWS]->()
     AND NOT has(n.visited)
     RETURN str(n.id) as id
     //ORDER BY length(p) asc, id desc
     LIMIT 1
   `).On(map[string]interface{}{}).Execute()
	if len(result.Data) == 0 {
		fmt.Println("didn't find user...")
		return User{Id: getSeedUser()}
	}
	if result.Error != nil {
		fmt.Println(result.Error)
	}
	//fmt.Println(result.Data[0].([]interface{})[0].(string))
	id, err := strconv.ParseUint(result.Data[0].([]interface{})[0].(string), 10, 64)
	if err != nil {
		fmt.Println(err)
	}
	return User{Id: id}
}

func getSeedUser() uint64 {
	c := oauth.NewConsumer(
		*consumerKey,
		*consumerSecret,
		oauth.ServiceProvider{
			RequestTokenUrl:   "http://api.twitter.com/oauth/request_token",
			AuthorizeTokenUrl: "https://api.twitter.com/oauth/authorize",
			AccessTokenUrl:    "https://api.twitter.com/oauth/access_token",
		})

	accessToken := &oauth.AccessToken{*accessTokenKey, *accessTokenSecret}

	resp, err := c.Get(
		"https://api.twitter.com/1.1/users/lookup.json",
		map[string]string{
			"screen_name": "wefreema",
		},
		accessToken)
	if err != nil {
		fmt.Println("error in user lookup", err)
	}
	users := []User{}
	err = json.NewDecoder(resp.Body).Decode(&users)
	if err != nil {
		fmt.Println("error in user lookup decode", err)
	}
	return users[0].Id
}

type FriendIdResponse struct {
	Ids        []uint64 `json:"ids"`
	NextCursor int64    `json:"next_cursor"`
}

func getFriends(user User, c *oauth.Consumer, accessToken *oauth.AccessToken) []User {
	fmt.Println("getFriends for user:", user)
	users := []User{}
	idsResp := FriendIdResponse{}
	idsResp.NextCursor = -1
	for {
		resp, err := c.Get(
			"https://api.twitter.com/1.1/friends/ids.json",
			map[string]string{
				"user_id": fmt.Sprintf("%d", user.Id),
				"count":   fmt.Sprintf("%d", 5000),
				"cursor":  fmt.Sprintf("%d", idsResp.NextCursor),
			},
			accessToken)
		if err != nil {
			fmt.Println("error in getFriends:", err)
			if strings.Contains(fmt.Sprintf("%s", err), "Unauthorized") {
				markAsVisited(user)
			}
			break
		} else {
			json.NewDecoder(resp.Body).Decode(&idsResp)
			if len(idsResp.Ids) == 0 {
				markAsVisited(user)
			}
			for _, id := range idsResp.Ids {
				users = append(users, User{Id: id})
			}
			if idsResp.NextCursor == 0 {
				break
			}
		}
	}

	return users
}

func markAsVisited(user User) {
	fmt.Println("marking as visited:", user.Id)
	result := g.Cypher(`
     MATCH (u:User)
     WHERE u.id = {id}
     SET u.visited = true
   `).On(map[string]interface{}{
		"id": user.Id,
	}).Execute()
	if result.Error != nil {
		fmt.Println(result.Error)
	}
}

func hydrateUsers() {
	for {
		users := getUsersToHydrate()
		hydrate(users)
		time.Sleep(15 * time.Second)
		fmt.Println("sleeping for 15 seconds")
	}
}

func hydrate(users []uint64) {
	c := oauth.NewConsumer(
		*consumerKey,
		*consumerSecret,
		oauth.ServiceProvider{
			RequestTokenUrl:   "http://api.twitter.com/oauth/request_token",
			AuthorizeTokenUrl: "https://api.twitter.com/oauth/authorize",
			AccessTokenUrl:    "https://api.twitter.com/oauth/access_token",
		})
	accessToken := &oauth.AccessToken{*accessTokenKey, *accessTokenSecret}

	userstrs := []string{}
	for _, u := range users {
		userstrs = append(userstrs, fmt.Sprintf("%d", u))
	}
	resp, err := c.Post(
		"https://api.twitter.com/1.1/users/lookup.json",
		map[string]string{
			"user_id": strings.Join(userstrs, ","),
		},
		accessToken)
	if err != nil {
		fmt.Println("error in user lookup", err)
	}

	us := []User{}
	err = json.NewDecoder(resp.Body).Decode(&us)
	if err != nil {
		fmt.Println("error in user lookup decode", err)
	}

	for _, user := range us {
		fmt.Println("hydrating:", user.Name)
		result := g.Cypher(`
        MERGE (u:User {id:{id}})
        ON MATCH SET u.screenName={screenName},
        u.description={description},
        u.name={name}
   `).On(map[string]interface{}{
			"id":          user.Id,
			"screenName":  user.ScreenName,
			"description": user.Description,
			"name":        user.Name,
		}).Execute()
		if result.Error != nil {
			fmt.Println("error in cypher:", result.Error)
		}
	}

}

func getUsersToHydrate() []uint64 {
	result := g.Cypher(`
     MATCH (u:User)
     WHERE NOT has(u.name)
     RETURN str(u.id)
     LIMIT 99 
   `).On(map[string]interface{}{}).Execute()
	if result.Error != nil {
		fmt.Println(result.Error)
	}
	//fmt.Println(result.Data[0].([]interface{})[0].(string))
	users := make([]uint64, 1)
	for _, d := range result.Data {
		i, err := strconv.ParseUint(d.([]interface{})[0].(string), 10, 64)
		if err != nil {
			fmt.Println(err)
		}
		users = append(users, i)
	}
	return users
}
