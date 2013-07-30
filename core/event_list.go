package core

//------------------------------------------------------------------------------
//
// Typedefs
//
//------------------------------------------------------------------------------

type EventList []*Event

//------------------------------------------------------------------------------
//
// Functions
//
//------------------------------------------------------------------------------

//--------------------------------------
// Sorting
//--------------------------------------

// Determines the length of an event slice.
func (s EventList) Len() int {
	return len(s)
}

// Compares two events in an event slice.
func (s EventList) Less(i, j int) bool {
	return s[i].Timestamp.Before(s[j].Timestamp)
}

// Swaps two events in an event slice.
func (s EventList) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

//--------------------------------------
// Cleaning
//--------------------------------------

// Returns a list of 
func (s EventList) NonEmptyEvents() EventList {
	events := make([]*Event, 0)
	for _, event := range s {
		if len(event.Data) > 0 {
			events = append(events, event)
		}
	}
	return EventList(events)
}
