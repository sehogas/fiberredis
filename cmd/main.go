package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/logger"
	"github.com/joho/godotenv"
	"github.com/redis/go-redis/v9"

	_ "github.com/denisenkom/go-mssqldb"
)

type GenericResponse struct {
	Status  bool        `json:"status" default:"false"`
	Message string      `json:"message,omitempty"`
	Data    interface{} `json:"data,omitempty"`
}

type BuquesEnPuertoResponse struct {
	Llegada     string  `db:"FecLlegada" json:"Llegada"`
	Buque       string  `db:"Buque" json:"Buque"`
	Tipo        string  `db:"Tipo" json:"Tipo"`
	Eslora      float32 `db:"Eslora" json:"Eslora"`
	Agente      string  `db:"Agente" json:"Agente"`
	Partida     string  `db:"Partida" json:"Partida"`
	Sitio       string  `db:"Sitio" json:"Sitio"`
	SitioImagen string  `db:"SitioImagen" json:"SitioImagen"`
	NumBuque    int     `db:"NumBuque" json:"NumBuque"`
}

var app *fiber.App
var cache *redis.Client
var db *sql.DB

var ctx context.Context

func main() {
	if err := godotenv.Load(); err != nil {
		log.Print("No .env file found")
	}

	app = fiber.New()
	app.Use(logger.New())

	cache = redis.NewClient(&redis.Options{
		Addr:     os.Getenv("REDIS_HOST") + ":" + os.Getenv("REDIS_PORT"),
		Password: os.Getenv("REDIS_PASSWORD"),
		DB:       0,
	})

	ctx = context.Background()

	ping := cache.Ping(ctx)
	response, err := ping.Result()
	if err != nil {
		log.Fatal("Error ping to redis")
	}
	fmt.Println(response)

	db, err = sql.Open(os.Getenv("DB_DRIVER"), os.Getenv("DB_SOURCE"))
	if err != nil {
		log.Fatal("No se pudo realizar conexión con la base de datos", err)
	}
	defer db.Close()

	if err = db.PingContext(ctx); err != nil {
		log.Fatal(err.Error())
	}

	app.Get("/", func(c *fiber.Ctx) error {
		return c.SendString("Hola mundo!")
	})
	app.Get("/buquesenpuerto", VerificarCache, EndpointBuquesEnPuerto)

	app.Listen(":3000")
}

func VerificarCache(c *fiber.Ctx) error {
	//id := c.Params("id")
	val, err := cache.Get(ctx, "BUQUES_EN_PUERTO").Bytes()
	if err != nil {
		return c.Next()
	}
	var data []BuquesEnPuertoResponse
	err = json.Unmarshal(val, &data)
	if err != nil {
		return c.Next()
	}
	return c.JSON(&GenericResponse{Status: true, Data: data})
}

func EndpointBuquesEnPuerto(c *fiber.Ctx) error {
	log.Println("Consultando base de datos...")
	data, err := ListBuquesEnPuerto(context.Background(), 1)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(&GenericResponse{Status: false, Message: err.Error()})
	}
	jsonData, err := json.Marshal(data)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(&GenericResponse{Status: false, Message: err.Error()})
	}
	cacheErr := cache.Set(ctx, "BUQUES_EN_PUERTO", jsonData, 10*time.Second).Err()
	if cacheErr != nil {
		return cacheErr
	}
	return c.Status(fiber.StatusOK).JSON(&GenericResponse{Status: true, Data: data})
}

func ListBuquesEnPuerto(ctx context.Context, numPuerto int) ([]BuquesEnPuertoResponse, error) {

	rows, err := db.QueryContext(ctx, "select Llegada, Buque, Tipo, Eslora, Agente, isnull(Partida,'') as Partida, Sitio, SitioImagen, NumBuque from vw_BuquesEnPuertoUshuaia2")
	if err != nil {
		return nil, err
	}

	defer rows.Close()
	var items []BuquesEnPuertoResponse
	for rows.Next() {
		var i BuquesEnPuertoResponse
		if err := rows.Scan(
			&i.Llegada,
			&i.Buque,
			&i.Tipo,
			&i.Eslora,
			&i.Agente,
			&i.Partida,
			&i.Sitio,
			&i.SitioImagen,
			&i.NumBuque,
		); err != nil {
			return nil, err
		}
		items = append(items, i)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}
