package com.crest.employee.leave.model;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
public class EmployeeLeaveRequest {

	private int employeeId;
	private int appliedLeaves;
}
