// Copyright 2012-2015 Oliver Eilhard. All rights reserved.
// Use of this source code is governed by a MIT-license.
// See http://quorumsco.mit-license.org/license.txt for details.

package elastic

type Filter interface {
	Source() interface{}
}
