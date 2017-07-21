val simpleScript =
"""
fileX = "";
fileY = "";
fileZ = "";

X = read (fileX);
Y = read (fileY);

Z = X %*% Y

write (Z,fileZ);
"""

// Generate data
val rawDataX = sqlContext.createDataFrame(LinearDataGenerator.generateLinearRDD(sc, 100, 10, 1))
val rawDataY = sqlContext.createDataFrame(LinearDataGenerator.generateLinearRDD(sc, 10, 100, 1))

// Repartition into a more parallelism-friendly number of partitions
val dataX = rawDataX.repartition(64).cache()
val dataY = rawDataY.repartition(64).cache()

// Create SystemML context
val ml = new MLContext(sc)

// Convert data to proper format
val mcX = new MatrixCharacteristics()
val mcY = new MatrixCharacteristics()
val X = RDDConverterUtils.vectorDataFrameToBinaryBlock(sc, dataX, mcX, false, "features")
val Y = RDDConverterUtils.vectorDataFrameToBinaryBlock(sc, dataY, mcY, false, "features")

// Register inputs & outputs
ml.reset()  
ml.registerInput("X", X, mcX)
ml.registerInput("Y", Y, mcY)
// ml.registerInput("y", y)
ml.registerOutput("Z")

val outputs = ml.executeScript(simpleScript)

// Get outputs
val Z = outputs.getDF(sqlContext, "Z")


Z.describe().show()