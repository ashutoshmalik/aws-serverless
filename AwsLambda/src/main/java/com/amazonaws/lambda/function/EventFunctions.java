package com.amazonaws.lambda.function;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.List;

import com.amazonaws.lambda.dao.DynamoDBEventDao;
import com.amazonaws.lambda.dynamodbevent.Event;
import com.amazonaws.lambda.pojo.City;
import com.amazonaws.lambda.pojo.Team;
import com.amazonaws.lambda.util.Consts;

public class EventFunctions {

	private static final Logger log = LogManager.getLogger(EventFunctions.class);

	private static final DynamoDBEventDao eventDao = DynamoDBEventDao.instance();

	public List<Event> getAllEventsHandler() {

		log.info("GetAllEvents invoked to scan table for all events");

		List<Event> events = eventDao.findAllEvents();		
		log.info("Found " + events.size() + " total events.");
		
		return events;
	}
	
	public List<Event> getEventsForTeam(Team team) throws UnsupportedEncodingException {
		
		if(null == team || team.getTeamName().isEmpty() || team.getTeamName().equals(Consts.UNDEFINED)) {
			log.error("GetEventsForTeam received null or empty team name");
			throw new IllegalArgumentException("Team name cannot be null or empty");
		}
		
		String name = URLDecoder.decode(team.getTeamName(), "UTF-8");
		log.info("GetEventsForTeam invoked for team with name = " + name);
		
		List<Event> events = eventDao.findEventByTeam(name);
		log.info("Found " + events.size() + " events for team = " + name);
		
		return events;
		
	}
	
	public List<Event> getEventsForCity(City city) throws UnsupportedEncodingException {
		
		if(null == city || city.getCityName().isEmpty() || city.getCityName().equals(Consts.UNDEFINED)) {
			log.error("GetEventsForCity received null or empty city name");
			throw new IllegalArgumentException("City name cannot be null or empty");
		}
		
		String name = URLDecoder.decode(city.getCityName(), "UTF-8");		
		log.info("GetEventsForTeam invoked for city with name = " + name);
		
		List<Event> events = eventDao.findEventsByCity(name);
		log.info("Found " + events.size() + " events for city = " + name);
		
		return events;
	}
	
	public void saveOrUpdateEvent(Event event) {
		
		if (null == event) {
			log.error("SaveEvent received null input");
			throw new IllegalArgumentException("Cannot save null object");
		}
		
		log.info("Saving or updating event for team = " + event.getHomeTeam() + ", date = " + event.getEventDate());
		eventDao.saveOrUpdateEvent(event);
		
		log.info("Successfully saved/updated event");
	}
	
	public void deleteEvent(Event event) {
		if (null == event) {
			log.error("DeleteEvent received null input");
			throw new IllegalArgumentException("Cannot delete null object");
		}
		
		log.info("Deleting event for team = " + event.getHomeTeam() + ", date = " + event.getEventDate());
		eventDao.deleteEvent(event.getHomeTeam(), event.getEventDate());
		log.info("Successfully deleted event");
	}
	
}
