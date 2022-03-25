package gravita

import (
	"regexp"
	"strings"
)

type matcher interface {
	Match(event *LambdaUDFEvent) bool
}

func (e *Entry) addMatcher(m matcher) *Entry {
	e.matchers = append(e.matchers, m)
	return e
}

// matchAllMacher is a Macher that matches any criteria
type matchAllMacher struct{}

func (_ matchAllMacher) Match(_ *LambdaUDFEvent) bool {
	return true
}

// ---- ExternalFunction ----

// externalFunctionMatcher matches the event external_function value
type externalFunctionMatcher string

// Match the event external_function value
func (m externalFunctionMatcher) Match(event *LambdaUDFEvent) bool {
	return string(m) == event.ExternalFunction
}

// ExternalFunction matches by LambdaUDF function name. You can use * as a wildcard
func (e *Entry) ExternalFunction(exFunc string) *Entry {
	if exFunc == "*" {
		return e.addMatcher(matchAllMacher{})
	}
	if strings.ContainsRune(exFunc, '*') {
		return e.ExternalFunctionRegexp(strings.ReplaceAll(exFunc, "*", ".*"))
	}
	return e.addMatcher(externalFunctionMatcher(exFunc))
}

type externalFunctionRegexpMatcher regexp.Regexp

// Match the event external_function value
func (m *externalFunctionRegexpMatcher) Match(event *LambdaUDFEvent) bool {
	return (*regexp.Regexp)(m).MatchString(event.ExternalFunction)
}

// ExternalFunctionRegexp matches a LambdaUDF function name with a regular expression
func (e *Entry) ExternalFunctionRegexp(expr string) *Entry {
	re := regexp.MustCompilePOSIX(expr)
	return e.addMatcher((*externalFunctionRegexpMatcher)(re))
}

// ---- User ----

// userMatcher matches the event user value
type userMatcher string

// Match the event user value
func (m userMatcher) Match(event *LambdaUDFEvent) bool {
	return string(m) == event.User
}

// User matches by LambdaUDF user name. You can use * as a wildcard
func (e *Entry) User(user string) *Entry {
	if user == "*" {
		return e.addMatcher(matchAllMacher{})
	}
	if strings.ContainsRune(user, '*') {
		return e.UserRegexp(strings.ReplaceAll(user, "*", ".*"))
	}
	return e.addMatcher(userMatcher(user))
}

type userRegexpMatcher regexp.Regexp

// Match the event user value
func (m *userRegexpMatcher) Match(event *LambdaUDFEvent) bool {
	return (*regexp.Regexp)(m).MatchString(event.User)
}

// UserRegexp matches a LambdaUDF user name with a regular expression
func (e *Entry) UserRegexp(expr string) *Entry {
	re := regexp.MustCompilePOSIX(expr)
	return e.addMatcher((*userRegexpMatcher)(re))
}

// ---- Cluster ----

// clusterMatcher matches the event cluster value
type clusterMatcher string

// Match the event cluster value
func (m clusterMatcher) Match(event *LambdaUDFEvent) bool {
	return string(m) == event.Cluster
}

// Cluster matches by LambdaUDF cluster name. You can use * as a wildcard
func (e *Entry) Cluster(cluster string) *Entry {
	if cluster == "*" {
		return e.addMatcher(matchAllMacher{})
	}
	if strings.ContainsRune(cluster, '*') {
		return e.ClusterRegexp(strings.ReplaceAll(cluster, "*", ".*"))
	}
	return e.addMatcher(clusterMatcher(cluster))
}

type clusterRegexpMatcher regexp.Regexp

// Match the event cluster value
func (m *clusterRegexpMatcher) Match(event *LambdaUDFEvent) bool {
	return (*regexp.Regexp)(m).MatchString(event.Cluster)
}

// ClusterRegexp matches a LambdaUDF cluster name with a regular expression
func (e *Entry) ClusterRegexp(expr string) *Entry {
	re := regexp.MustCompilePOSIX(expr)
	return e.addMatcher((*clusterRegexpMatcher)(re))
}

// ---- Database ----

// databaseMatcher matches the event database value
type databaseMatcher string

// Match the event database value
func (m databaseMatcher) Match(event *LambdaUDFEvent) bool {
	return string(m) == event.Database
}

// Database matches by LambdaUDF database name. You can use * as a wildcard
func (e *Entry) Database(database string) *Entry {
	if database == "*" {
		return e.addMatcher(matchAllMacher{})
	}
	if strings.ContainsRune(database, '*') {
		return e.DatabaseRegexp(strings.ReplaceAll(database, "*", ".*"))
	}
	return e.addMatcher(databaseMatcher(database))
}

type databaseRegexpMatcher regexp.Regexp

// Match the event database value
func (m *databaseRegexpMatcher) Match(event *LambdaUDFEvent) bool {
	return (*regexp.Regexp)(m).MatchString(event.Database)
}

// DatabaseRegexp matches a LambdaUDF database name with a regular expression
func (e *Entry) DatabaseRegexp(expr string) *Entry {
	re := regexp.MustCompilePOSIX(expr)
	return e.addMatcher((*databaseRegexpMatcher)(re))
}
