#!/usr/bin/env ruby
require 'rubygems'
require 'pg'
require 'builder'
require 'getopt/std'
require './string_hstore'

# writes the XML representation of one node.
def write_node(xml_parent, row, include_tags)
    # Some entries have nulled X/Y coords. They will break the import process.
    if (!row["x"] || !row["y"])
        return
    end
    if (include_tags)
        xml_parent.node(
          :id => row["osm_id"], 
          :user => "dummy", :uid => 0, 
          :timestamp => "2012-01-01T00:00:00Z", 
          :version => "1", 
          :lat => row["y"], :lon => row["x"]) do |node|
            $fields.each do |key|
                next if row[key] == nil
                node.tag(:k => key, :v => row[key])
            end
            if (row['tags'])
                tags = row['tags'].from_hstore
                tags.each do |key,value|
                    if (key != 'way_area')
                        node.tag(:k => key, :v => value)
                    end
                end
            end
        end
    else
        xml_parent.node(
          :id => row["osm_id"], 
          :user => "dummy", :uid => 0, 
          :timestamp => "2012-01-01T00:00:00Z", 
          :version => "1", 
          :lat => row["y"], :lon => row["x"])
    end
end

def create_diff

    xml = Builder::XmlMarkup.new(:target=>STDOUT, :indent => 2 )
    xml.instruct! :xml, :encoding => "UTF-8"
    xml.osmChange do |osc|
        $conn.transaction do

            # "delete" section in the osmChange document - select all pseudo_nodes
            # that have a delete flag set, output them, and delete them from the
            # table.
            osc.delete do |del|
                begin
                    # read max 1000 objects at a time
                    res = $conn.exec('select osm_id, tags, ' + $fields.join(',') + ',st_x(way) as x,st_y(way) as y from pseudo_nodes where deleted=true and dirty=false limit 1000')
                    rows=0
                    ids = Array.new()
                    res.each do |row|
                        write_node(del, row, false)
                        ids.push(row["osm_id"])
                        rows = rows + 1
                    end
                    res.clear()
                    # delete records
                    if (rows > 0) 
                        res = $conn.exec('delete from pseudo_nodes where osm_id in(' + ids.join(',') + ')')
                        res.clear()
                    end
                end while rows>0
            end

            # "modify" section in the osmChange document - select all pseudo_nodes
            # with dirty flag set, output them, and clear the dirty flag.
            osc.modify do |mod|
                begin
                    # read max 1000 objects at a time
                    res = $conn.exec('select osm_id, tags, ' + $fields.join(',') + ',st_x(way) as x,st_y(way) as y from pseudo_nodes where dirty=true and deleted=true limit 1000')
                    rows=0
                    ids = Array.new()
                    res.each do |row|
                        write_node(mod, row, true)
                        ids.push(row["osm_id"])
                        rows = rows + 1
                    end
                    res.clear()
                    # clear dirty flag
                    if (rows > 0) 
                        res = $conn.exec('update pseudo_nodes set dirty=false,deleted=false where osm_id in(' + ids.join(',') + ')')
                        res.clear()
                    end
                end while rows>0
            end

            # "create" section in the osmChange document - select all pseudo_nodes
            # with dirty flag set, output them, and clear the dirty flag.
            osc.create do |mod|
                begin
                    # read max 1000 objects at a time
                    res = $conn.exec('select osm_id, tags, ' + $fields.join(',') + ',st_x(way) as x,st_y(way) as y from pseudo_nodes where dirty=true and deleted=false limit 1000')
                    rows=0
                    ids = Array.new()
                    res.each do |row|
                        write_node(mod, row, true)
                        ids.push(row["osm_id"])
                        rows = rows + 1
                    end
                    res.clear()
                    # clear dirty flag
                    if (rows > 0) 
                        res = $conn.exec('update pseudo_nodes set dirty=false where osm_id in(' + ids.join(',') + ')')
                        res.clear()
                    end
                end while rows>0
            end
        end
    end

end

# this procedure creates an osmChange document that contains *all* the 
# pseudo_nodes in the database, wrapped in a <create>...</create> block.
# use it when setting up the "area poi" feature,
# or when re-synching for any reason.
#
# if the database from which you create dump is not freshly created, then 
# you should do this before the dump:
#   delete from pseudo_nodes where deleted=true;
#   update pseudo_nodes set dirty=false;

def create_dump
    xml = Builder::XmlMarkup.new(:target=>STDOUT, :indent => 2 )
    xml.instruct! :xml, :encoding => "UTF-8"
    xml.osmChange do |osc|
        osc.create do |cre|
            $conn.transaction do
                $conn.exec('declare mycursor cursor for select osm_id, tags, ' + $fields.join(',') + ',st_x(way) as x,st_y(way) as y from pseudo_nodes')
                begin
                    res = $conn.exec('fetch forward 1000 from mycursor')
                    break if (res.cmd_tuples() == 0)
                    res.each do |row|
                       write_node(cre, row, true) 
                    end
                end while true
            end
        end
    end
end

$conn = PGconn.open(:dbname => 'osm', :user => 'osm', :password => 'osm', :host => 'osm-database')
$fields = [ 'shop', 'office', 'aerialway', 'aeroway', 'amenity', 'tourism', 'historic', 'sport', 'leisure', 'public_transport' ]

opt = Getopt::Std.getopts("fd")

if opt['f']
   create_dump
elsif opt['d']
   create_diff
else
   puts "please use one of -f (full dump) or -d (diff)"
end 

