package com.amazonaws.lambda.dao;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazonaws.lambda.dynamodbevent.Event;
import com.amazonaws.lambda.manager.DynamoDBManager;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBQueryExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBScanExpression;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;

public class DynamoDBEventDao implements EventDao {
	
	private static final Logger log = LogManager.getLogger(DynamoDBEventDao.class);

	private static final DynamoDBMapper mapper = DynamoDBManager.mapper();

	private static volatile DynamoDBEventDao instance;
	
	private DynamoDBEventDao() {
	
	}
	
	public static DynamoDBEventDao instance() {
		if(instance == null) {
			synchronized (DynamoDBEventDao.class) {
				if (instance == null)
					instance = new DynamoDBEventDao();
			}
		}
		return instance;
	}
	
	@Override
	public List<Event> findAllEvents() {
		return mapper.scan(Event.class, new DynamoDBScanExpression());
	}

	@Override
	public List<Event> findEventsByCity(String cityName) {
		
		Map<String, AttributeValue> eav = new HashMap<>();
		eav.put(":v1", new AttributeValue().withS(cityName));
		
		DynamoDBQueryExpression<Event> query = new DynamoDBQueryExpression<Event>()
												.withIndexName(Event.CITY_INDEX)
												.withConsistentRead(false)
												.withKeyConditionExpression("city = :v1")
												.withExpressionAttributeValues(eav);
		
		return mapper.query(Event.class, query);
		
	}

	@Override
	public List<Event> findEventByTeam(String teamName) {
		
		Event eventKey = new Event();
		eventKey.setHomeTeam(teamName);
		
		DynamoDBQueryExpression<Event> homeQuery = new DynamoDBQueryExpression<Event>();
		homeQuery.setHashKeyValues(eventKey);
		
		List<Event> homeEvents = mapper.query(Event.class, homeQuery);
		
		Map<String, AttributeValue> eav = new HashMap<>();
		eav.put(":v1", new AttributeValue().withS(teamName));
		
		DynamoDBQueryExpression<Event> awayQuery = new DynamoDBQueryExpression<Event>()
														.withIndexName(Event.AWAY_TEAM_INDEX)
														.withConsistentRead(false)
														.withKeyConditionExpression("awayTeam = :v1")
														.withExpressionAttributeValues(eav);
		
		List<Event> awayEvents = mapper.query(Event.class, awayQuery);
		
		List<Event> allEvents = new LinkedList<Event>();
		allEvents.addAll(homeEvents);
		allEvents.addAll(awayEvents);
		
		allEvents.sort((e1, e2) -> e1.getEventDate() <= e2.getEventDate() ? -1 : 1);
		
		return allEvents;
	}

	@Override
	public Optional<Event> findEventByTeamAndDate(String teamName, Long eventDate) {
		Event event = mapper.load(Event.class, teamName, eventDate);
		return Optional.ofNullable(event);
	}

	@Override
	public void saveOrUpdateEvent(Event event) {
		mapper.batchSave(event);
	}

	@Override
	public void deleteEvent(String teamName, Long eventDate) {

		Optional<Event> oEvent = findEventByTeamAndDate(teamName, eventDate);
		
		if (oEvent.isPresent()) {
			mapper.delete(oEvent.get());
		} else {
			log.error("Could not delete event, no such team and date combination");
			throw new IllegalArgumentException("Could not delete event, no such team and date combination");
		}
	}

}
