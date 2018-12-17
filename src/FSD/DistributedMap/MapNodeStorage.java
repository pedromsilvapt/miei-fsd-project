package FSD.DistributedMap;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MapNodeStorage {
    private String name;

    public MapNodeStorage ( String name ) {
        this.name = name;
    }

    public String getName () {
        return name;
    }

    public void setName ( String name ) {
        this.name = name;
    }

    private String getDataFolder () {
        try {
            URI uri = MapNodeStorage.class.getProtectionDomain().getCodeSource().getLocation().toURI();

            return Paths.get( new File( uri ).getPath(), "data" ).toString();
        } catch ( URISyntaxException e ) {
            return "";
        }
    }

    private String getFileNameForKey ( long key ) {
        return Paths.get( this.getDataFolder(), Long.toString( key ) ).toString();
    }

    private void makeDataFolder () {
        File folder = new File( this.getDataFolder() );

        if ( !folder.exists() ) {
            folder.mkdirs();
        }
    }

    public void put ( long key, byte[] data ) throws IOException {
        this.makeDataFolder();

        Files.write( new File( this.getFileNameForKey( key ) ).toPath(), data );
    }

    public void putAll ( Map< Long, byte[] > data ) throws IOException {
        for ( Map.Entry< Long, byte[] > entry : data.entrySet() ) {
            this.put( entry.getKey(), entry.getValue() );
        }
    }

    public byte[] get ( long key ) {
        try {
            String filename = this.getFileNameForKey( key );

            File file = new File( filename );

            if ( file.exists() && file.isFile() ) {
                return Files.readAllBytes( file.toPath() );
            }

            return null;
        } catch ( IOException e ) {
            return null;
        }
    }

    public Map< Long, byte[] > getAll () {
        Map< Long, byte[] > data = new HashMap<>();

        for ( long key : this.keys() ) {
            data.put( key, this.get( key ) );
        }

        return data;
    }

    public Iterable< Long > keys () {
        final String folder = this.getDataFolder();

        return new Iterable< Long >() {
            @Override
            public Iterator< Long > iterator () {
                return new Iterator< Long >() {
                    private List< Long > keys = null;
                    private boolean started = false;
                    private int index = 0;

                    private void load () {
                        if ( !this.started ) {
                            this.started = true;

                            if ( folder != null ) {
                                try ( Stream< Path > paths = Files.walk( Paths.get( folder ) ) ) {
                                    this.keys = paths
                                            .map( Path::toFile )
                                            .filter( File::isFile )
                                            .map( file -> Long.parseLong( file.getName() ) )
                                            .collect( Collectors.toList() );
                                } catch ( IOException e ) {
                                    this.keys = new ArrayList<>();
                                }
                            } else {
                                this.keys = new ArrayList<>();
                            }
                        }
                    }

                    @Override
                    public boolean hasNext () {
                        this.load();

                        return this.index < this.keys.size();
                    }

                    @Override
                    public Long next () {
                        this.load();

                        return this.keys.get( this.index++ );
                    }
                };
            }
        };
    }
}
