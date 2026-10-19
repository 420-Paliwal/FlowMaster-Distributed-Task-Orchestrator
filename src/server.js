const { producer, connectProducer } = require("./kafka");
const express = require("express");
const { randomUUID } = require("crypto");
const pool = require("./db");

const app = express();
const PORT = process.env.PORT || 3000;

app.use(express.json());

app.get("/health", async (req, res) => {
  try {
    await pool.query("SELECT 1");
    res.json({
      status: "ok",
      database: "connected",
    });
  } catch (error) {
    res.status(503).json({
      status: "error",
      message: "Database unavailable",
    });
  }
});

app.post("/workflows", async (req, res) => {
  const { workflow_type, input = {} } = req.body;

  if (
    typeof workflow_type !== "string" ||
    !workflow_type.trim() ||
    typeof input !== "object" ||
    input === null ||
    Array.isArray(input)
  ) {
    return res.status(400).json({
      error: "workflow_type and an object input are required",
    });
  }

  const id = randomUUID();
  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    await client.query(
      `INSERT INTO workflows
       (id, workflow_type, status, input, current_step)
       VALUES ($1, $2, $3, $4, $5)`,
      [id, workflow_type.trim(), "PENDING", JSON.stringify(input), "verification"]
    );

    await client.query(
      `INSERT INTO workflow_events
       (workflow_id, event_type, step_name, payload)
       VALUES ($1, $2, $3, $4)`,
      [
        id,
        "WORKFLOW_CREATED",
        "verification",
        JSON.stringify({ workflow_type: workflow_type.trim() }),
      ]
    );

    await client.query("COMMIT");

    // Publish workflow event to Kafka
    await producer.send({
      topic: "workflow.commands",
      messages: [
        {
          key: id,
          value: JSON.stringify({
            eventType: "WORKFLOW_STARTED",
            workflowId: id,
            workflowType: workflow_type.trim(),
            input,
          }),
        },
      ],
    });

    return res.status(201).json({
      id,
      workflow_type: workflow_type.trim(),
      status: "PENDING",
      message: "Workflow created",
    });
  } catch (error) {
    await client.query("ROLLBACK");
    console.error("Create workflow failed:", error.message);
    return res.status(500).json({ error: "Could not create workflow" });
  } finally {
    client.release();
  }
});

app.get("/workflows/:id", async (req, res) => {
  try {
    const result = await pool.query(
      "SELECT * FROM workflows WHERE id = $1",
      [req.params.id]
    );

    if (result.rowCount === 0) {
      return res.status(404).json({ error: "Workflow not found" });
    }

    const events = await pool.query(
      `SELECT id, event_type, step_name, payload, created_at
       FROM workflow_events
       WHERE workflow_id = $1
       ORDER BY id ASC`,
      [req.params.id]
    );

    return res.json({
      workflow: result.rows[0],
      events: events.rows,
    });
  } catch (error) {
    console.error("Get workflow failed:", error.message);
    return res.status(500).json({ error: "Could not retrieve workflow" });
  }
});

async function startServer() {
  try {
    await connectProducer();

    app.listen(PORT, () => {
      console.log(`FlowMaster API running on http://localhost:${PORT}`);
    });
  } catch (error) {
    console.error("Failed to start server:", error.message);
    process.exit(1);
  }
}

startServer();