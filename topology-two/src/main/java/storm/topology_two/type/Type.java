package storm.topology_two.type;

public enum Type {
 
	STRING("STRING"),
	NUMBER("NUMBER"),
	SIGN("SIGN");
	
	public final String name;
	
	private Type(String name) {
		// TODO Auto-generated constructor stub
		this.name = name;
	}
	
	public final String get(){
		return name;
	}
	
}
