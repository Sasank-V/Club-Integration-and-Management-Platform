package controllers

import (
	"log"
	"sync"

	"github.com/Sasank-V/CIMP-Golang-Backend/database"
	"github.com/Sasank-V/CIMP-Golang-Backend/database/schemas"
	"github.com/Sasank-V/CIMP-Golang-Backend/lib"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

var ClubColl *mongo.Collection
var ClubConnect sync.Once

func ConnectClubCollection() {
	ClubConnect.Do(func() {
		db := database.InitDB()
		schemas.CreateClubCollection(db)
		ClubColl = db.Collection(lib.ClubCollName)
	})
}

// Get Functions

func GetClubByID(id string) (schemas.Club, error) {
	ctx, cancel := database.GetContext()
	defer cancel()

	var club schemas.Club
	err := ClubColl.FindOne(ctx, bson.D{{"id", id}}).Decode(&club)
	if err != nil {
		log.Printf("error getting club data: %v", err)
		return schemas.Club{}, err

	}
	return club, nil
}

// GetClubsByIDs fetches multiple clubs in a single batch query
func GetClubsByIDs(ids []string) (map[string]schemas.Club, error) {
	if len(ids) == 0 {
		return make(map[string]schemas.Club), nil
	}

	ctx, cancel := database.GetLongContext()
	defer cancel()

	filter := bson.M{"id": bson.M{"$in": ids}}
	cursor, err := ClubColl.Find(ctx, filter)
	if err != nil {
		log.Printf("error fetching clubs in batch: %v", err)
		return nil, err
	}
	defer cursor.Close(ctx)

	var clubs []schemas.Club
	if err = cursor.All(ctx, &clubs); err != nil {
		log.Printf("cursor error in GetClubsByIDs: %v", err)
		return nil, err
	}

	// Create a map for quick lookup
	clubMap := make(map[string]schemas.Club, len(clubs))
	for _, club := range clubs {
		clubMap[club.ID] = club
	}

	return clubMap, nil
}
