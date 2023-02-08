// ##### CASO 1 #####

db.sensores.insert([{
    "ubicacion": "Sevilla",
    "medidas_sensor": [{"tipo_medida":"Temperatura", "unidad":"ºC"},
                      {"tipo_medida":"Humedad_relativa", "unidad":"%"}],
    "coordenadas": [37.409311, -5.949939],
    "fecha_instalacion": new Date("2020-05-28T11:30:00Z"),
    "location_id": 2,
    "tipo_sensor": 1
},
{
    "ubicacion": "Sevilla",
    "medidas_sensor": [{"tipo_medida":"Emision_CO2", "unidad":"gCO2/m2"},
                      {"tipo_medida":"Consumo_electrico", "unidad":"kWh/m2"}],
    "coordenadas": [37.409311, -5.949939],
    "fecha_instalacion": new Date("2020-05-28T11:30:00Z"),
    "location_id": 2,
    "tipo_sensor": 2
},
{
    "ubicacion": "Valladolid",
    "medidas_sensor": [{"tipo_medida":"Temperatura", "unidad":"ºC"},
                      {"tipo_medida":"Humedad_relativa", "unidad":"%"}],
    "coordenadas": [41.638597, 4.740186],
    "fecha_instalacion": new Date("2020-05-25T10:30:00Z"),
    "location_id": 1,
    "tipo_sensor": 1
},
{
    "ubicacion": "Valladolid",
    "medidas_sensor": [{"tipo_medida":"Emision_CO2", "unidad":"gCO2/m2"},
                      {"tipo_medida":"Consumo_electrico", "unidad":"kWh/m2"}],
    "coordenadas": [41.638597, 4.740186],
    "fecha_instalacion": new Date("2020-05-25T10:30:00Z"),
    "location_id": 1,
    "tipo_sensor": 2
}])


// ##### CASO 2 #####

// Recogidos en Valladolid
db.datos_sensores.find({"location_id": 1, 
                  "medidas.tipo_medida": {$in:["Temperatura"]},
                  "timestamp":{$in:[/2020-07-0/, /2020-07-10/]}
}).count()


// Recogidos en Sevilla
db.datos_sensores.find({"location_id": 2, 
                  "medidas.tipo_medida": {$in:["Temperatura"]},
                  "timestamp":{$in:[/2020-07-0/, /2020-07-10/]}
}).count()


// Para ver qué fecha es en la que no se tiene datos:

var consulta_fechas_valladolid = db.datos_sensores.distinct("timestamp", 
                          {
                            "location_id": 1,
                    	      "medidas.tipo_medida": {$in:["Temperatura"]},
                    	      "timestamp":{$in:[/2020-07-0/, /2020-07-10/]}
                          }
                        );
                        
var consulta_fechas_sevilla = db.datos_sensores.distinct("timestamp", 
                          {
                            "location_id": 2,
                    	      "medidas.tipo_medida": {$in:["Temperatura"]},
                    	      "timestamp":{$in:[/2020-07-0/, /2020-07-10/]}
                          }
                        );

var fechas_unicas = consulta_fechas_valladolid.filter(function(value) { return consulta_fechas_sevilla.indexOf(value) == -1; });

print(fechas_unicas);


// ##### CASO 3 #####

db.datos_sensores.aggregate([
  {
    $unwind: "$medidas"
  },
  {
    $match: {
      $and: [
        {
          "medidas.tipo_medida": "Temperatura",
          "medidas.valor": {$gt: 55}
        }
      ]
    }
  },
  {
      $project: {
         _id: 0,
         timestamp: 1, 
         location_id: 1,
         medidas: 1
      }
   }
]).pretty();


db.datos_sensores.aggregate([
  {
    $unwind: "$medidas"
  },
  {
    $match: {
      $and: [
        {
          "medidas.tipo_medida": "Temperatura",
          "medidas.valor": {$gt: 55}
        }
      ]
    }
  },
  {
    $group: {
      _id: "$location_id",
      count: {
        $sum: 1
      },
      docs: { $push: {
          "location_id": "$location_id",
          "medidas": "$medidas",
          "timestamp": "$timestamp"
        }
      }
    }
  }
]).pretty();


db.datos_sensores.deleteMany({
  "medidas": {
    $elemMatch: {
      "tipo_medida": "Temperatura",
      "valor": {$gt: 55}
    }
  }
});


// ##### CASO 4 #####

# Encontrar el valor mínimo
db.datos_sensores.aggregate([
   {$match: {"location_id": 2}},
   {$unwind: "$medidas"},
   {$match: {"medidas.tipo_medida": "Temperatura"}},
   {$group: {
      _id: "$location_id",
      minTemperatura: {$min: "$medidas.valor"}
   }}
]);

// Mostrar el documento completo
db.datos_sensores.find({"location_id": 2, 
                  "medidas.tipo_medida": {$in:["Temperatura"]},
                  "medidas.valor":9.14
}).pretty()

// Multiplicar el mínimo por 1.2
db.datos_sensores.update(
   {location_id: 2, "medidas.tipo_medida": "Temperatura", "medidas.valor": 9.14},
   {$mul: {"medidas.$.valor": 1.2}}
);

// Buscar el documento nuevamente para validar el cambio
db.datos_sensores.findOne({"_id": ObjectId("63bc7b3dbf67c2221d99bd02")})


// ##### CASO 5 #####

var fecha_inicio = ISODate("2020-07-04T00:00:00Z");
var fecha_fin = ISODate("2020-07-05T00:00:00Z");

db.datos_sensores.aggregate([
  {
    "$addFields": 
    {
      "timestamp": 
      {
        "$toDate": "$timestamp" 
      }
    }
  },
  {
    $match: {
      timestamp: {
        $gte: fecha_inicio,
        $lte: fecha_fin
      }
    }
  },
  {
    $unwind: "$medidas"
  },
  {
    $match: {
      "medidas.tipo_medida": "Consumo_electrico"
    }
  },
  {
    $group: {
      _id: "$location_id",
      datos: {
        $push: {
          timestamp: "$timestamp",
          valor: "$medidas.valor",
          unidad: "$medidas.unidad",
          diaSemana: {
            $dateToString: {
              format: "%w",
              date: "$timestamp"
            }
          },
          hora: {
            $dateToString: {
              format: "%H:%M",
              date: "$timestamp"
            }
          }
        }
      }
    }
  },
  {
    $unwind: "$datos"
  },
  {
    $sort: {
      "datos.valor": -1
    }
  },
  {
    $group: {
      _id: "$_id",
      datos: {
        $push: "$datos"
      }
    }
  },
  {
    $sort: {
      "_id": 1
    }
  },
  {
    $project: {
      _id: 0,
      location_id: "$_id",
      datos: {
        $slice: ["$datos", 3]
      }
    }
  }
]).pretty()


// ##### CASO 6 #####

// Para Valladolid - Número de veces que se supera el límite

db.datos_sensores.aggregate([
  {
    "$addFields": 
    {
      "timestamp": 
      {
        "$toDate": "$timestamp" 
      }
    }
  },
  {
    $unwind: "$medidas"
  },
  {
    $match: {
      "medidas.tipo_medida": "Emision_CO2",
      "location_id": 1
    }
  },
  {
    $group: {
      _id: {
        anio: {$year: "$timestamp"},
        mes: {$month: "$timestamp"},
        dia: {$dayOfMonth: "$timestamp"}
      },
      suma_Emision_CO2: {$sum: "$medidas.valor"}
    }
  },
  {
    $match: {
      suma_Emision_CO2: {$gt: 420}
    }
  },
  {
    $count: "días_sobre_limite_permitido"
  }
]).pretty()


// Para Sevilla - Número de veces que se supera el límite

db.datos_sensores.aggregate([
  {
    "$addFields": 
    {
      "timestamp": 
      {
        "$toDate": "$timestamp" 
      }
    }
  },
  {
    $unwind: "$medidas"
  },
  {
    $match: {
      "medidas.tipo_medida": "Emision_CO2",
      "location_id": 2
    }
  },
  {
    $group: {
      _id: {
        anio: {$year: "$timestamp"},
        mes: {$month: "$timestamp"},
        dia: {$dayOfMonth: "$timestamp"}
      },
      suma_Emision_CO2: {$sum: "$medidas.valor"}
    }
  },
  {
    $match: {
      suma_Emision_CO2: {$gt: 420}
    }
  },
  {
    $count: "días_sobre_limite_permitido"
  }
]).pretty()


// Para Valladolid - Documentos ordenados de mayor a emisión que superan el límite

db.datos_sensores.aggregate([
  {
    "$addFields": 
    {
      "timestamp": 
      {
        "$toDate": "$timestamp" 
      }
    }
  },
  {
    $unwind: "$medidas"
  },
  {
    $match: {
      "medidas.tipo_medida": "Emision_CO2",
      "location_id": 1
    }
  },
  {
    $group: {
      _id: {
        anio: {$year: "$timestamp"},
        mes: {$month: "$timestamp"},
        dia: {$dayOfMonth: "$timestamp"}
      },
      suma_Emision_CO2: {$sum: "$medidas.valor"}
    }
  },
  {
    $match: {
      suma_Emision_CO2: {$gt: 420}
    }
  },
  {
    $sort: {
      "suma_Emision_CO2": -1
    }
  }
]).pretty()


// Para Sevilla - Documentos ordenados de mayor a emisión que superan el límite

db.datos_sensores.aggregate([
  {
    "$addFields": 
    {
      "timestamp": 
      {
        "$toDate": "$timestamp" 
      }
    }
  },
  {
    $unwind: "$medidas"
  },
  {
    $match: {
      "medidas.tipo_medida": "Emision_CO2",
      "location_id": 2
    }
  },
  {
    $group: {
      _id: {
        anio: {$year: "$timestamp"},
        mes: {$month: "$timestamp"},
        dia: {$dayOfMonth: "$timestamp"}
      },
      suma_Emision_CO2: {$sum: "$medidas.valor"}
    }
  },
  {
    $match: {
      suma_Emision_CO2: {$gt: 420}
    }
  },
  {
    $sort: {
      "suma_Emision_CO2": -1
    }
  }
]).pretty()


// ##### CASO 7 #####

db.datos_sensores.aggregate([
  {
    $unwind: "$medidas"
  },
  {
    $match: {
      "medidas.tipo_medida": "Emision_CO2",
      "timestamp": {
        $regex: ".*T10:.*"
      }
    }
  },
  {
    $group: {
      _id: "$location_id",
      avg_emision_CO2: {
        $avg: "$medidas.valor"
      }
    }
  },
  {
    $project: {
      _id: 0,
      location_id: "$_id",
      avg_emision_CO2: {
        $round: ["$avg_emision_CO2", 2]
      }
    }
  },
  {
    $sort: {
      avg_emision_CO2: 1
    }
  }
]).pretty()



// ##### CASO 8 #####


db.datos_sensores.aggregate([
    {
      $match: { 
        "sensor_id": 1, 
        "location_id": 1 
      }
    },
    {
      $project: {
        "_id": 0,
        "timestamp": 1,
        "location_id": 1,
        "medidas": { 
          "$arrayElemAt": [ "$medidas", 0 ] 
        }
      }
    },
    {
      $sort: {
        "medidas.valor": -1
      }
    },
    {
      $limit: 2
    }
]).pretty()


db.datos_sensores.updateMany(
  {
    "sensor_id": 1,
    "location_id": 1,
    "medidas.tipo_medida": "Temperatura"
  },
  {
    $inc: {"medidas.$.valor": -1.5}
  }
);



// ##### CASO 9 #####


db.datos_sensores.aggregate([
  {
    "$addFields": { "timestamp": {"$toDate": "$timestamp"} }
  },
  {
    "$addFields": { "hora": { "$dateToString": { "format": "%H", "date": "$timestamp" } } }
  },
  {
    "$addFields": { "hora": { $toInt: "$hora" } }
  },
  {
    $unwind: "$medidas"
  },
  {
    $match: {
      $and: [
        {
          hora: {
            $gte: 8,
            $lte: 18
          }
        },
        {
          "medidas.tipo_medida": "Humedad_relativa"
        }
      ]
    }
  },
  {
    $sort: {
      "medidas.valor": 1
    }
  },
  {
    $group: {
      _id: "$location_id",
      docs_por_ciudad: {
        $push: {
          timestamp: "$timestamp",
          tipo_medida: "$medidas.tipo_medida",
          valor: "$medidas.valor",
          unidad: "$medidas.unidad"
        }
      }
    }
  },
  {
    $project: {
      _id: 0,
      location_id: "$_id",
      datos: {
        $slice: ["$docs_por_ciudad", 5]
      }
    }
  },
  {
    $sort: {
      "location_id": -1
    }
  }
]).pretty()


// ##### CASO 10 #####


// Valladolid

db.datos_sensores.updateMany(
  { 
    "sensor_id": 2, 
    "location_id": 1
  },
  {
    $addToSet: 
    { 
      "medidas": { $each: [ 
          { "precio_kWh": 0.102, "unidad": "€/kWh" },
          {"superficie":450,"unidad":"m2"}
        ]
      }
    } 
  }
)


db.datos_sensores.find({
  $or: [
    { "timestamp": "2020-07-01T08:00:00Z", "location_id": 1 },
    { "timestamp": "2020-07-01T23:15:00Z", "location_id": 1 }
  ]
}).limit(4).pretty()


// Sevilla

db.datos_sensores.updateMany(
  { 
    "sensor_id": 2, 
    "location_id": 2
  },
  {
    $addToSet: 
    { 
      "medidas": { $each: [ 
          { "precio_kWh": 0.107, "unidad": "€/kWh" },
          {"superficie": 550, "unidad": "m2"}
        ]
      }
    } 
  }
)


db.datos_sensores.find({
  $or: [
    { "timestamp": "2020-07-01T08:00:00Z", "location_id": 2 },
    { "timestamp": "2020-07-01T23:15:00Z", "location_id": 2 }
  ]
}).limit(4).pretty()


