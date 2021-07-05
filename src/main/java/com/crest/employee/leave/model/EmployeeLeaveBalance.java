package com.crest.employee.leave.model;

import org.apache.commons.lang3.StringUtils;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
public class EmployeeLeaveBalance {

	private int employeeId;
	private String employeeName;
	private int leavesTaken;
	private int availableLeaves;

	@Override
	public String toString() {
		return StringUtils.join(employeeId, ",", employeeName, ",", leavesTaken, ",", availableLeaves);
	}
}
