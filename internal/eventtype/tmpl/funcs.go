// Package tmpl provides the template helper functions shared by the Telegram
// adapter and per-event render tests. It must not import adapter internals —
// event packages import this, and the adapter imports event packages.
package tmpl

import "text/template"

var FuncMap = template.FuncMap{
	// get traverses nested map[string]any by successive keys; returns nil if any step is missing.
	"get": func(m map[string]any, keys ...string) any {
		var cur any = m
		for _, k := range keys {
			node, ok := cur.(map[string]any)
			if !ok {
				return nil
			}
			cur = node[k]
		}
		return cur
	},
	// str is like get but returns "" if the value is missing or not a string.
	"str": func(m map[string]any, keys ...string) string {
		var cur any = m
		for _, k := range keys {
			node, ok := cur.(map[string]any)
			if !ok {
				return ""
			}
			cur = node[k]
		}
		s, _ := cur.(string)
		return s
	},
}
