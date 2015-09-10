package models

import "time"

type Note struct {
	ID      uint       `db:"id" json:"id"`
	Content string     `db:"content" json:"content"`
	Author  string     `db:"author" json:"author"`
	Date    *time.Time `db:"date" json:"date"`

	GroupID   uint `db:"group_id" json:"group_id"`
	ContactID uint `db:"contact_id" json:"contact_id"`
}

type NoteArgs struct {
	GroupID   uint
	ContactID uint
	Note      *Note
}

type NoteReply struct {
	Note  *Note
	Notes []Note
}
