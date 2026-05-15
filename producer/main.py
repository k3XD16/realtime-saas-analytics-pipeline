import asyncio
from fastapi import FastAPI, HTTPException, BackgroundTasks
from loguru import logger
from .config import Config
from .event_generator import EventGenerator
from .kafka_producer import KafkaProducer

app = FastAPI(title="SaaS Analytics Producer")

# Initialize Producer as a singleton
try:
    Config.validate()
    producer = KafkaProducer()
except Exception as e:
    logger.critical(f"Failed to initialize producer: {e}")
    producer = None

@app.get("/health")
def health_check():
    """Returns the health status of the producer."""
    if producer:
        return {"status": "healthy", "kafka": "connected"}
    return {"status": "unhealthy", "kafka": "disconnected"}, 503

@app.post("/generate-event")
async def generate_single_event(event_type: str = None):
    """Generates and sends a single event to Kafka."""
    if not producer:
        raise HTTPException(status_code=503, detail="Kafka producer not initialized")
    
    event = EventGenerator.generate_event(event_type)
    producer.send_event(event)
    return {"message": "Event sent", "event_id": event['event_id'], "type": event['event_type']}

async def simulation_task(count: int, delay: float):
    """Background task to simulate a stream of events."""
    for i in range(count):
        event = EventGenerator.generate_event()
        producer.send_event(event)
        await asyncio.sleep(delay)
    producer.flush()
    logger.info(f"Simulation of {count} events completed.")

@app.post("/simulate")
async def start_simulation(background_tasks: BackgroundTasks, count: int = 100, delay: float = 0.5):
    """Triggers a background simulation of multiple events."""
    if not producer:
        raise HTTPException(status_code=503, detail="Kafka producer not initialized")
    
    background_tasks.add_task(simulation_task, count, delay)
    return {"message": f"Started simulation of {count} events with {delay}s delay."}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
