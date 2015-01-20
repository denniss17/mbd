package nl.utwente.bigdata.util;

public class Score {
	
	public Score(int s1, int s2) {
		T1goals = s1;
		T2goals = s2;
	}
	public boolean equals(Score s2) {
		if(this.T1goals != s2.T1goals) return false;
		if(this.T2goals != s2.T2goals) return false;
		return true;
	}
	
	public int T1goals;
	public int T2goals;
	
	@Override
	public String toString() {
		return "[" + T1goals + " - " + T2goals + "]";
	}
}
