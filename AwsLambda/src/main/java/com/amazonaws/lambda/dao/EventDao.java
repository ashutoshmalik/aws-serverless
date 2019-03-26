package com.amazonaws.lambda.dao;

import java.util.List;
import java.util.Optional;

import com.amazonaws.lambda.dynamodbevent.Event;

public interface EventDao {

	List<Event> findAllEvents();
	
	List<Event> findEventsByCity(String cityName);
	
	List<Event> findEventByTeam(String teamName);
	
	Optional<Event> findEventByTeamAndDate(String teamName, Long eventDate);
	
	void saveOrUpdateEvent(Event event);
	
	void deleteEvent(String teamName, Long eventDate);
	
}
